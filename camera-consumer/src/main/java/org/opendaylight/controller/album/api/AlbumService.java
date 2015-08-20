package org.opendaylight.controller.album.api;

import java.util.concurrent.Future;

import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.camera.rev091120.PhotoType;
import org.opendaylight.yangtools.yang.common.RpcResult;

public interface AlbumService {

    Future<RpcResult<Void>> makeAlbum( PaperType paper, Class<? extends PhotoType> photoType, int exposure );

}
