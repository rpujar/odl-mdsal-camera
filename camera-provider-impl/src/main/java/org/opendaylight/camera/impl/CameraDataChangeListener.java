package org.opendaylight.camera.impl;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.binding.api.WriteTransaction;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataBroker.DataChangeScope;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraParams;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraParams.CameraStatus;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.CameraParamsBuilder;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CameraDataChangeListener implements DataChangeListener, AutoCloseable {

    //Through YANG file I added a configuration parameter called brightnessFactor,
    //so the Provider class needs to listen to changes made to this config param.
    //This class is invoked my the CameraModule class from onSessionInitiated()
    // i.e. CameraProvider/CameraDataChangeListener must implement DataChangeListener's onDataChanged() do define the behavior.
    // Also, modify code for any operation that needs to use this parameter, i.e. ClickPhotoTask.
    // Finally, register our listener to DataProviderService
    //CHECK!!! If you need to update default-config.xml with binding for datachangelistener service - mostly NO

    private static final Logger LOG = LoggerFactory
            .getLogger(CameraDataChangeListener.class);
    private DataBroker db;
    private static CameraStatus oldCameraStatus; ///TODO RASHMI!! BAD

    public CameraDataChangeListener(DataBroker dataBroker, CameraStatus oldStatus) {
        this.db=dataBroker;
        this.oldCameraStatus=oldStatus;//TODO RASHMI!! BAD
        //Registering the listener for changes made to CONFIG datastore
        final ListenerRegistration<DataChangeListener> dataChangeListenerRegistration =
                db.registerDataChangeListener(LogicalDatastoreType.CONFIGURATION, CameraProvider.CAMERA_IID, this, DataChangeScope.SUBTREE);
        LOG.info("CameraDataChangeListener Registered");
    }

    /**
     * This method is used to notify the CameraProvider when a data change
     * is made to the configuration.
     *
     * Effectively, the camera subtree node is modified through restconf
     * and the onDataChanged is triggered. We check if the changed dataObject is
     * from the camera subtree, if so we get the value of the brightnessFactor.
     *
     * If the brightnessFactor from the node is not null, then we change the brightnessFactor of
     * the CameraProvider with the subtree brightnessFactor.
     */
    @Override
    public void onDataChanged(
            AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
        //Note 1: DataObject will contain only the config parameter value from the RESTConf for config i.e. brightnessFactor only
        //Note 2: Transaction.put updates the entire tree, if a node is null it doesn't add it to the tree though
        //Note 3: There is no implementation needed to do a GET request from RESTConf for Operational & Config Datastore.
        DataObject dataObject = change.getUpdatedSubtree();
        if (dataObject instanceof CameraParams) {
            Long brightnessFactor = ((CameraParams) dataObject).getBrightnessFactor();
            if (brightnessFactor!=null) {
                //Rebuild CameraParams obj with current values plus set the brightnessFactor & store in Operational DataStore
                LOG.info("@brightnessFactor:" +brightnessFactor);//+ " @cameraStatus:" +((CameraParams) change.getOriginalSubtree()).getCameraStatus());
                CameraParams cameraParams = new CameraParamsBuilder()
                        .setCameraManufacturer(CameraProvider.CAMERA_MANUFACTURER)
                        .setCameraModelNumber(CameraProvider.CAMERA_MODEL)
                        .setCameraStatus(oldCameraStatus)//(((CameraParams) change.getOriginalSubtree()).getCameraStatus())
                        .setBrightnessFactor(brightnessFactor)
                        .build();
                WriteTransaction transaction = db.newWriteOnlyTransaction();
                transaction.put(LogicalDatastoreType.OPERATIONAL, CameraProvider.CAMERA_IID, cameraParams);
                transaction.submit();
            }
        }
    }


    @Override
    public void close() throws Exception {
        LOG.info("CameraDataChangeListener closed");

    }

}
