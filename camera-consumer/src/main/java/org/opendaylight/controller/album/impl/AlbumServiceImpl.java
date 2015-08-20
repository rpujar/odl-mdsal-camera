package org.opendaylight.controller.album.impl;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.opendaylight.controller.album.api.AlbumService;
import org.opendaylight.controller.album.api.PaperType;
import org.opendaylight.controller.config.yang.config.album_service.impl.AlbumServiceRuntimeMXBean;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ConsumerContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareConsumer;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraOutOfPhotos;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraRestocked;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.ClickPhotoInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.ClickPhotoInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.Color;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.PhotoType;
import org.opendaylight.yangtools.yang.common.RpcError;
import org.opendaylight.yangtools.yang.common.RpcError.ErrorType;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.common.RpcResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class AlbumServiceImpl implements AlbumService, AutoCloseable, BindingAwareConsumer, AlbumServiceRuntimeMXBean,CameraListener {

    private static final Logger log = LoggerFactory.getLogger( AlbumServiceImpl.class );
    private final CameraService camera;
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private DataBroker dataBroker;
    private volatile boolean cameraOutOfPhotos;

    public AlbumServiceImpl(CameraService camera) {
        this.camera = camera;
    }

    @Override
    public void onSessionInitialized(ConsumerContext session) {
        this.dataBroker =  session.getSALService(DataBroker.class);

    }

    @Override
    public Future<RpcResult<Void>> makeAlbum(PaperType paper,
            Class<? extends PhotoType> photoType, int exposure) {
        // Call clickPhoto and use JdkFutureAdapters to convert the Future to a ListenableFuture,
        // The OpendaylightToaster impl already returns a ListenableFuture so the conversion is
        // actually a no-op.

        log.info("In makeAlbum()");
        ListenableFuture<RpcResult<Void>> clickPhotoFuture = JdkFutureAdapters.listenInPoolThread(
                clickPhoto( photoType, exposure ), executor );
        ListenableFuture<RpcResult<Void>> makeAlbumFuture = fetchPaper(paper);

        // Combine the 2 ListenableFutures into 1 containing a list of RpcResults and transform RpcResults into 1
        ListenableFuture<List<RpcResult<Void>>> combinedFutures =
                Futures.allAsList( ImmutableList.of( clickPhotoFuture, makeAlbumFuture ) );

        return Futures.transform(combinedFutures,
                new AsyncFunction<List<RpcResult<Void>>,RpcResult<Void>>() {
            @Override
            public ListenableFuture<RpcResult<Void>> apply( List<RpcResult<Void>> results ) throws Exception {
                boolean atLeastOneSucceeded = false;
                Builder<RpcError> errorList = ImmutableList.builder();
                for( RpcResult<Void> result: results ) {
                    if( result.isSuccessful() ) {
                        atLeastOneSucceeded = true;
                    }
                    if( result.getErrors() != null ) {
                        errorList.addAll( result.getErrors() );
                    }
                }
                return Futures.immediateFuture(
                        RpcResultBuilder.<Void> status(atLeastOneSucceeded).withRpcErrors(errorList.build()).build());
            }
        } );
    }

    private ListenableFuture<RpcResult<Void>> fetchPaper(PaperType paper) {
        log.info("In fetchPaper()");
        return executor.submit(new Callable<RpcResult<Void>>(){
            @Override
            public RpcResult<Void> call() throws Exception {
                // we don't actually do anything here, just return a successful result.
                return RpcResultBuilder.<Void> success().build();
            }
        });
    }

    private Future<RpcResult<Void>> clickPhoto(
            Class<? extends PhotoType> photoType, int exposure) {
        if(cameraOutOfPhotos)
        {
            log.info("Out of photos but we can make a default album");
            return Futures.immediateFuture(RpcResultBuilder.<Void> success().withWarning(ErrorType.APPLICATION, "partial operation", "Out of photos but we can make a default album").build());
        }
        ClickPhotoInput clickPhotoInput = new ClickPhotoInputBuilder().setExposure(exposure).setPhotoType(photoType).build();
        return camera.clickPhoto(clickPhotoInput);
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }

    @Override
    public Boolean makeMatteWithColorRpc() {
        try {
            //This call has to block since we must return a result to the JMX client.
            RpcResult<Void> result = makeAlbum(PaperType.MATTE, Color.class, 2).get();
            if(result.isSuccessful()) {
                log.info( "makeMatteWithColorRpc succeeded" );
            } else {
                log.info( "makeMatteWithColorRpc failed: " + result.getErrors() );
            }
            return result.isSuccessful();
        } catch(InterruptedException | ExecutionException e ) {
            log.info( "An error occurred while maing breakfast: " + e );
        }
        return Boolean.FALSE;
    }

    @Override
    public void onCameraOutOfPhotos(CameraOutOfPhotos notification) {
        log.info("CameraOutOfPhotos notification");
        cameraOutOfPhotos=true;
    }

    @Override
    public void onCameraRestocked(CameraRestocked notification) {
        log.info("CameraRestocked notification-numberOfPhotos" +notification.getNumberOfPhotos());
        cameraOutOfPhotos=false;
    }
}
