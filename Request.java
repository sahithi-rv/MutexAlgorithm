
import java.rmi.*;
public interface Request extends Remote {
	com.google.protobuf.ByteString LockRequest(com.google.protobuf.ByteString data) throws RemoteException;
	void UnlockRequest(com.google.protobuf.ByteString data) throws RemoteException;
}
