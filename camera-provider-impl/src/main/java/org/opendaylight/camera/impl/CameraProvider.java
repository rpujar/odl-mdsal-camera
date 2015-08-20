/*
 * Copyright (c) Rashmi 2015 and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.camera.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.ReadWriteTransaction;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.md.sal.common.api.data.OptimisticLockFailedException;
import org.opendaylight.controller.md.sal.common.api.data.TransactionCommitFailedException;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.impl.rev141210.CameraRuntimeMXBean;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraOutOfPhotosBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraParams;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraParams.CameraStatus;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraParamsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraRestocked;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraRestockedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.ClickPhotoInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.DisplayString;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.RestockCameraInput;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcError;
import org.opendaylight.yangtools.yang.common.RpcError.ErrorType;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class CameraProvider implements BindingAwareProvider, AutoCloseable,
CameraService, CameraRuntimeMXBean {

    private static final Logger LOG = LoggerFactory
            .getLogger(CameraProvider.class);
    public static final InstanceIdentifier<CameraParams> CAMERA_IID = InstanceIdentifier
            .builder(CameraParams.class).build();
    static final DisplayString CAMERA_MANUFACTURER = new DisplayString(
            "Opendaylight");
    static final DisplayString CAMERA_MODEL = new DisplayString(
            "Model 1 BindingAware");
    private DataBroker db;
    private final ExecutorService executor;
    private volatile CameraStatus status = CameraStatus.Off;
    private AtomicInteger exposure = new AtomicInteger(0);
    public AtomicInteger brightnessFactor = new AtomicInteger(0);
    private final AtomicLong photosClicked = new AtomicLong(0);
    private final AtomicLong numberOfPhotosAvailable = new AtomicLong(0);
    private CameraDataChangeListener cameraDataChangeListener;
    private NotificationProviderService notificationProvider;

    // The following holds the Future for the current make toast task.
    // This is used to cancel the current toast.
    private final AtomicReference<Future<?>> currentClickPhotoTask = new AtomicReference<>();

    public CameraProvider() {
        executor = Executors.newFixedThreadPool(1);
    }

    /*
     * @Override(non-Javadoc)
     *
     * @see org.opendaylight.controller.sal.binding.api.BindingAwareProvider#
     * onSessionInitiated INITIALIZATION --> On initialization, it writes the
     * operational toaster data to the MD-SAL's data store, via the DataBroker
     * interface, and deletes the data on close. When the session is started:
     * Get the DataBroker for the provider session, and add the CameraParams to
     * both the Operational and Config Datastore
     */
    public void onSessionInitiated(ProviderContext session) {
        LOG.info("CameraProvider Session Initiated");
        db = session.getSALService(DataBroker.class);
        init();
        syncCameraParamWithDataStore(LogicalDatastoreType.OPERATIONAL,
                CAMERA_IID, buildCameraParams(status));
        // syncCameraParamWithDataStore(LogicalDatastoreType.CONFIGURATION,CAMERA_IID,buildCameraParams(status));
    }

    public void setNotificationProvider(NotificationProviderService salService) {
        this.notificationProvider = salService;
    }

    private void init() {
        cameraDataChangeListener = new CameraDataChangeListener(db, this.status); 
        //To-Do: don't want to be sending the status here

    }

    /*
     * Default function to build a camera returns a CameraParams object using
     * the CameraParamsBuilder().build()
     */
    private CameraParams buildCameraParams(CameraStatus camStatus) {
        return new CameraParamsBuilder()
        .setCameraManufacturer(CAMERA_MANUFACTURER)
        .setCameraModelNumber(CAMERA_MODEL).setCameraStatus(camStatus)
        .build();
    }

    @SuppressWarnings({ "unused", "rawtypes", "unchecked" })
    private void syncCameraParamWithDataStore(final LogicalDatastoreType store,
            InstanceIdentifier iid, final DataObject object) {
        WriteTransaction transaction = db.newWriteOnlyTransaction();
        transaction.put(store, iid, object);
        //Perform the tx.submit asynchronously
        Futures.addCallback(transaction.submit(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(final Void result) {
                LOG.info("SyncStore {} with object {} succeeded", store, object);
                //To-Do: notifyCallback(true);
            }

            //          //To-Do:
            //            private void notifyCallback(boolean b) {
            //                if (resultCallback != null) {
            //                    resultCallback.apply(b);
            //            }

            @Override
            public void onFailure(final Throwable throwable) {
                LOG.error("SyncStore {} with object {} failed", store, object);
                //                //To-Do: notifyCallback(false);
            }
        });
    }

    /*
     * @Override(non-Javadoc)
     *
     * @see java.lang.AutoCloseable#close()
     */
    public void close() throws Exception {
        executor.shutdown(); // part 2
        if (db != null) {
            WriteTransaction transaction = db.newWriteOnlyTransaction();
            transaction.delete(LogicalDatastoreType.OPERATIONAL, CAMERA_IID);
            Futures.addCallback(transaction.submit(),
                    new FutureCallback<Void>() {

                @Override
                public void onFailure(final Throwable t) {
                    LOG.error("Failed to delete camera" + t);
                }

                @Override
                public void onSuccess(final Void result) {
                    LOG.debug("Successfully deleted camera" + result);
                }
            });
        }

        if (cameraDataChangeListener!=null) {
            cameraDataChangeListener.close();
            LOG.info("CameraDataChangeListener Closed");
        }

        LOG.info("CameraProvider Closed");
    }

    /**
     * Uses the yangtools.yang.common.RpcResultBuilder to return a cancel camera
     * Future. CameraProvider implements CameraService; cancelPhoto() and
     * clickPhoto() needs to be overridden
     */
    @Override
    public Future<RpcResult<Void>> cancelPhoto() {
        Future<?> current = currentClickPhotoTask.getAndSet(null);
        if (current != null) {
            current.cancel(true);
        }
        // Always return success from the cancel toast call.
        return Futures.immediateFuture(RpcResultBuilder.<Void> success()
                .build());
    }

    @Override
    public Future<RpcResult<Void>> clickPhoto(ClickPhotoInput input) {
        LOG.info("In clickPhoto()");
        final SettableFuture<RpcResult<Void>> futureResult = SettableFuture
                .create();
        checkStatusandClickPhoto(input, futureResult, 2);
        return futureResult;
    }

    /**
     * Read the CameraStatus and, if currently Off, try to write the status to
     * On. If that succeeds, then we essentially have an exclusive lock and can
     * proceed to click the photo.
     *
     * @param input
     * @param futureResult
     * @param tries
     */
    private void checkStatusandClickPhoto(final ClickPhotoInput input,
            final SettableFuture<RpcResult<Void>> futureResult, final int tries) {
        /*
         * We create a ReadWriteTransaction by using the databroker. Then, we
         * read the status of the camera with getCameraStatus() using the
         * databroker again. Once we have the status, we analyze it and then
         * databroker submit function is called to effectively change the camera
         * status. This all affects the MD-SAL tree, more specifically the part
         * of the tree that contain the camera (the nodes).
         */
        LOG.info("In checkStatusandClickPhoto()");
        final ReadWriteTransaction tx = db.newReadWriteTransaction();
        ListenableFuture<Optional<CameraParams>> readFuture = tx.read(
                LogicalDatastoreType.OPERATIONAL, CAMERA_IID);

        final ListenableFuture<Void> commitFuture = Futures.transform(
                readFuture, new AsyncFunction<Optional<CameraParams>, Void>() {

                    @SuppressWarnings("deprecation")
                    @Override
                    public ListenableFuture<Void> apply(
                            Optional<CameraParams> cameraParamsData)
                                    throws Exception {
                        // TODO Auto-generated method stub
                        if (cameraParamsData.isPresent()) {
                            status = cameraParamsData.get().getCameraStatus();
                        } else {
                            throw new Exception(
                                    "Error reading CameraParams data from the store.");
                        }
                        LOG.info("Read camera status: {}", status);
                        if (status == CameraStatus.Off) {
                            //Check if numberOfPhotosAvailable is not 0, if yes Notify outOfStock
                            if(numberOfPhotosAvailable.get() == 0) {
                                LOG.info("No more photos availble for clicking");
                                notificationProvider.publish(new CameraOutOfPhotosBuilder().build());
                                return Futures.immediateFailedCheckedFuture(
                                        new TransactionCommitFailedException("", clickNoMorePhotosError()));
                            }
                            LOG.info("Setting Camera status to On");
                            // We're not currently clicking photo - try to
                            // update the status to On
                            // to indicate we're going to click photo. This acts
                            // as a lock to prevent
                            // concurrent clicking.
                            tx.put(LogicalDatastoreType.OPERATIONAL,
                                    CAMERA_IID,
                                    buildCameraParams(CameraStatus.On));
                            return tx.submit();
                        }

                        LOG.info("Oops - already clicking photo!");
                        // Return an error since we are already clicking photo.
                        // This will get
                        // propagated to the commitFuture below which will
                        // interpret the null
                        // TransactionStatus in the RpcResult as an error
                        // condition.
                        return Futures
                                .immediateFailedCheckedFuture(new TransactionCommitFailedException(
                                        "", clickPhotoInUseError()));
                    }

                    private RpcError clickNoMorePhotosError() {
                        return RpcResultBuilder.newError( ErrorType.APPLICATION, "resource-denied",
                                "No more photos available for clicking", "out-of-stock", null, null );
                    }
                });
        Futures.addCallback(commitFuture, new FutureCallback<Void>() {

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof OptimisticLockFailedException) {
                    // Another thread is likely trying to click a photo
                    // simultaneously and updated the
                    // status before us. Try reading the status again - if
                    // another click-photo is
                    // now in progress, we should get CameraStatus.Off and fail.
                    if ((tries - 1) > 0) {
                        LOG.info("Got OptimisticLockFailedException - trying again");
                        checkStatusandClickPhoto(input, futureResult, tries - 1);
                    } else {
                        futureResult.set(RpcResultBuilder
                                .<Void> failed()
                                .withError(ErrorType.APPLICATION,
                                        t.getMessage()).build());
                    }
                } else {
                    LOG.info("Failed to commit Camera status", t);
                    // Probably already clicking a photo.
                    futureResult.set(RpcResultBuilder
                            .<Void> failed()
                            .withRpcErrors(
                                    ((TransactionCommitFailedException) t)
                                    .getErrorList()).build());
                }
            }

            @Override
            public void onSuccess(Void result) {
                // OK to click a photo
                currentClickPhotoTask.set(executor.submit(new ClickPhotoTask(
                        input, futureResult)));

            }

        });
    }

    private RpcError clickPhotoInUseError() {
        return RpcResultBuilder.newWarning(ErrorType.APPLICATION, "in use",
                "Camera is busy (in-use)", null, null, null);
    }

    private class ClickPhotoTask implements Callable<Void> {
        final ClickPhotoInput photoRequest;
        final SettableFuture<RpcResult<Void>> futureResult;

        public ClickPhotoTask(ClickPhotoInput photoRequest,
                SettableFuture<RpcResult<Void>> futureResult) {
            this.photoRequest = photoRequest;
            this.futureResult = futureResult;
        }

        @SuppressWarnings("deprecation")
        @Override
        public Void call() throws Exception {
            // click photo sleeps for n seconds for every 1 unit change of
            // exposure level
            LOG.info("Inside ClickPhotoTask's call() method");
            LOG.info("Exposure is:" + photoRequest.getExposure());
            if(photoRequest.getPhotoType() != null)
                LOG.info("PhotoType is:" +photoRequest.getPhotoType().getName());
            try {
                int exposure = CameraProvider.this.exposure.get();

                //Get the brightness constant if present
                int bF=1;
                if(CameraProvider.this.brightnessFactor!=null)
                    bF = CameraProvider.this.brightnessFactor.get();
                Thread.sleep(Math.abs(bF*(exposure + photoRequest.getExposure())));
            } catch (InterruptedException e) {
                LOG.info("Interrupted while clicking photo");
            }
            numberOfPhotosAvailable.getAndDecrement();
            // need to redo this even though it is already handled in checkStatusAndCLickPhoto before it gets here.
            //since if the first time getAndDecrement() on numberOfPhotos gets called and is 0 then publish Notif
            if(numberOfPhotosAvailable.get()==0) {
                LOG.info("Camera out of Memory to click photos!!!!");
                notificationProvider.publish(new CameraOutOfPhotosBuilder().build());
            }
            photosClicked.incrementAndGet();
            syncCameraParamWithDataStore(LogicalDatastoreType.OPERATIONAL,
                    CAMERA_IID, buildCameraParams(CameraStatus.On));

            // Set the Camera status back to off - this essentially releases the
            // photo clicking lock.
            // We can't clear the current click photo task nor set the Future
            // result until the
            // update has been committed so we pass a callback to be notified on
            // completion.

            //To-Do:  Instead of Sleep here, add a callback to syncCameraParamWithDataStore
            //to return a boolean to let you know about the completion of click-photo,
            //if true then only reset the CameraStatus to off
            Thread.sleep(10);
            setCameraStatusOff(new Function<Boolean, Void>() {
                @Override
                public Void apply(Boolean result) {
                    currentClickPhotoTask.set(null);
                    LOG.info("Camera Ready @setCameraStatusOff.apply()");
                    futureResult.set(RpcResultBuilder.<Void> success().build());
                    return null;
                }
            });
            return null;
        }

        private void setCameraStatusOff(
                final Function<Boolean, Void> resultCallback) {
            WriteTransaction tx = db.newWriteOnlyTransaction();
            tx.put(LogicalDatastoreType.OPERATIONAL, CAMERA_IID,
                    buildCameraParams(CameraStatus.Off));

            Futures.addCallback(tx.submit(), new FutureCallback<Void>() {

                @Override
                public void onFailure(Throwable t) {
                    LOG.error("Failed to reset the Camera Status to Off");
                    notifyCallback(false);
                }

                @Override
                public void onSuccess(Void arg0) {
                    LOG.info("Reset Camera Status to Off");
                    notifyCallback(true);
                }

                private void notifyCallback(boolean b) {
                    if (resultCallback != null) {
                        resultCallback.apply(b);
                    }
                }
            });
        }
    }

    //JMX RPC call implemented from the CameraRuntimeMXBean interface.
    @Override
    public Long getPhotosClicked() {
        // TODO Auto-generated method stub
        return photosClicked.get();
    }

    //JMX RPC call implemented from the CameraRuntimeMXBean interface.
    @Override
    public void clearPhotosClickedRpc() {
        LOG.info( "In Clear Photos Clicked RPC" );
        photosClicked.set(0);
    }

    //RPC call implemented from the CameraService interface.
    @SuppressWarnings("deprecation")
    @Override
    public Future<RpcResult<Void>> restockCamera(RestockCameraInput input) {
        LOG.info("currentNumberOfPhotos:" +numberOfPhotosAvailable.get()+ "  restockCamera:" +input);
        Long currentNumberOfPhotos = numberOfPhotosAvailable.get();
        numberOfPhotosAvailable.set(currentNumberOfPhotos+input.getAmountOfPhotosToStock());
        //Store numberOfPhotosAvailable in OP-DataStore TO-DO RASHMI
        if(input.getAmountOfPhotosToStock()>0) {
            CameraRestocked camRestockedNotif= new CameraRestockedBuilder().setNumberOfPhotos(input.getAmountOfPhotosToStock()).build();
            notificationProvider.publish(camRestockedNotif);
        }

        return Futures.immediateFuture( RpcResultBuilder.<Void> success().build() );
    }
}
