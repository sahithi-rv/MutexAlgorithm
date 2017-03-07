import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;



public class MyProg implements Request {
	
	public MyProg()throws RemoteException {
		
	};
	
	public static int ParseOutputFile(String line){
		int cnt=0;
		if(line.length()>0){
			String parsed_string[] = line.split(":");
			cnt = Integer.parseInt(parsed_string[0]);
		}
		return cnt;
	}
	
	public static boolean check_all_acks(boolean[] ack_flag){
		for(int i = 1;i<=Nodes; i++){
			if(!ack_flag[i])
				return false;
		}
		return true;
	}
	
	public static boolean HasLock(){ 
		return LockFlag;
	}
	
	public static boolean DoIWantLock(){
		for(int i = 0;i<q.size();i++){
			if(q.get(i).getPid() == MyId){
				WantLock = true;
				return true;
			}
		}
		WantLock = false;
		LockFlag = false;
		return false;
	}
	
	public static RequestMessageProtos.VectorClock.Builder getClock(){
		return vector_clock;
	}
	
	public static void setClock(RequestMessageProtos.VectorClock.Builder c){
		vector_clock = c;
	}
	
	// update my clock according to the receiver's clock
	static void UpdateMyClock(RequestMessageProtos.VectorClock.Builder my_vclk,RequestMessageProtos.VectorClock senders_clock){
		
		RequestMessageProtos.Clock sclk ;//= senders_clock.getVectorClock(senders_pid);
		RequestMessageProtos.Clock my_clk;// = my_vclk.getVectorClock(MyId);
		/*for( RequestMessageProtos.Clock clk : my_vclk.getVectorClockList() ){
			
		}*/
		//my_clk.setTimer( my_clk.getTimer() + 1 );
		RequestMessageProtos.Clock.Builder a = RequestMessageProtos.Clock.newBuilder();
		RequestMessageProtos.Clock.Builder b = RequestMessageProtos.Clock.newBuilder();
		for(int i = 1;i<= Nodes; i++){
			sclk = senders_clock.getVectorClock(i);
			my_clk = my_vclk.getVectorClock(i);
			if( i!=MyId && sclk.getTimer() > my_clk.getTimer() ){
				a.setPid(i);
				a.setTimer(sclk.getTimer());
				my_vclk.setVectorClock(i, a);
			}
		}
		/*my_clk = my_vclk.getVectorClock(MyId);
		a.setPid(MyId);
		a.setTimer(my_clk.getTimer() + 1);
		my_vclk.setVectorClock(MyId, a);*/
		setClock(my_vclk);
	}
		
	public static boolean clock_compare(RequestMessageProtos.Clock c1, RequestMessageProtos.Clock c2){
		if(c1.getTimer()<c2.getTimer()){
			return false;
		}else if(c1.getTimer()==c2.getTimer()){
			if(c1.getPid()<c2.getPid())
				return false;
			return true;
		}
		return true;
	}
	
	public static void AddQueue(RequestMessageProtos.Clock new_reqclock){
		RequestMessageProtos.Clock qclk;
		int i=0;
		for(; i<q.size(); i++){
			qclk = q.get(i);
			if(qclk.getPid() == new_reqclock.getPid())
				break;
			if(clock_compare(new_reqclock, qclk)){
				;
			}else{
				q.add(i, new_reqclock);
				break;
			}
		}
		if(i== q.size()){
			q.add(new_reqclock);
		}
	}
	
	public static String getQElements(){
		String s;
		s = "";
		for(int i = 0;i < q.size(); i++){
			s+= Integer.toString(q.get(i).getPid());
			s+=" ";
		}
		return s;
	}
	
	public static ByteString Ack_q(boolean ok, boolean qok){
		 RequestMessageProtos.Acknowledgement.Builder ack_builder = RequestMessageProtos.Acknowledgement.newBuilder();
		 ack_builder.setOk(ok);
		 ack_builder.setQueuedOk(qok);
		 ack_builder.setSenderClock(getClock());
		 RequestMessageProtos.Acknowledgement ack = ack_builder.build();
		 return (ack.toByteString());
	}
	
	public static String clockToList(RequestMessageProtos.VectorClock vclk ){
		String s = "";
		for(int i = 1;i<=Nodes; i++){
			int val = vclk.getVectorClock(i).getTimer();
			s += Integer.toString(val) + " ";
		}		
		return s;
		
	}
	
	@Override
	public ByteString LockRequest(ByteString data) throws RemoteException {
		try{
			@SuppressWarnings("unused")
			RequestMessageProtos.VectorClock senders_clock = RequestMessageProtos.VectorClock.parseFrom(data);
			int sender_rpid = senders_clock.getRpid();
			RequestMessageProtos.VectorClock.Builder my_vclk = getClock();
			RequestMessageProtos.Clock mclk = my_vclk.getVectorClock(MyId);
			
			/*increment yours*/
			 RequestMessageProtos.Clock.Builder clk;
			 clk = increment_clock();
			 my_vclk.setVectorClock(MyId, clk);
			 setClock(my_vclk);
			
			 /*** updating at the beg ***/
			 UpdateMyClock(my_vclk,senders_clock);
			System.out.print(Integer.toString(MyId) + " received lock request from " + Integer.toString(sender_rpid) + " at " );
			System.out.println( clockToList( my_vclk.build()) );
			
			if(DoIWantLock()){
				AddQueue(senders_clock.getVectorClock(sender_rpid));
			}
			
			System.out.print("in lock req q.size ");
			System.out.println(q.size());
			System.out.println(getQElements());
			/*increment yours*/
			 clk = increment_clock();
			 my_vclk.setVectorClock(MyId, clk);
			 setClock(my_vclk);
			
			if(HasLock()){
				System.out.print(Integer.toString(MyId) + " sending queued ok to " + Integer.toString(sender_rpid) + " for lock req " );
				System.out.println(clockToList(senders_clock));
				return Ack_q(false, true);
			}else{
				try{
					if(WantLock){
						RequestMessageProtos.Clock sclk = senders_clock.getVectorClock(sender_rpid);
						if(clock_compare(sclk, mclk)){
							System.out.print(Integer.toString(MyId) + " sending queued ok to " + Integer.toString(sender_rpid) + " for lock req " );
							System.out.println(clockToList(senders_clock));
							return Ack_q(false, true);
							 
							 
						}else{
							System.out.print(Integer.toString(MyId) + " sending ok to " + Integer.toString(sender_rpid) + " for lock req " );
							System.out.println(clockToList(senders_clock));
							 return Ack_q(true, false);
						}
					}else{		
						System.out.print(Integer.toString(MyId) + " sending ok to " + Integer.toString(sender_rpid) + " for lock req " );
						System.out.println(clockToList(senders_clock));
						 return Ack_q(true, false);
					}
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			//UpdateMyClock(my_vclk,senders_clock);
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void UnlockRequest(ByteString data) throws RemoteException {
		// TODO Auto-generated method stub
		try{
			@SuppressWarnings("unused")
		
			RequestMessageProtos.VectorClock senders_clock = RequestMessageProtos.VectorClock.parseFrom(data);
			int sender_rpid = senders_clock.getRpid();
			RequestMessageProtos.VectorClock.Builder my_vclk = getClock();
		
			/*increment yours*/
			 RequestMessageProtos.Clock.Builder mclk;
			 mclk = increment_clock();
			 my_vclk.setVectorClock(MyId, mclk);
			 setClock(my_vclk);
			 
			 UpdateMyClock(my_vclk,senders_clock);
			 
			System.out.print(Integer.toString(MyId) + " received unlock req from " + Integer.toString(sender_rpid) + " at");
			System.out.println(clockToList(my_vclk.build()));
			System.out.print("q.size() ");
			System.out.println(q.size());
			System.out.println(getQElements());
			for(int i = 0;i< q.size(); i++){
				RequestMessageProtos.Clock clk = q.get(i);
				if(clk.getPid() == sender_rpid){
					System.out.println("removing " + Integer.toString(sender_rpid) + "from q ");
					q.remove(i);
					//UpdateMyClock(my_vclk,senders_clock);
					if(/* q.get(0).getPid() == MyId ||*/ WantLock){
						System.out.println("i want lock ");
						System.out.println(Integer.toString(MyId) + " to ok ack from " + Integer.toString(sender_rpid) + " at");
						
						ack_flag[sender_rpid] = true;
					}
					break;
				}
			}
						
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
		
		
	}
		
	public static RequestMessageProtos.Clock.Builder increment_clock(){
		RequestMessageProtos.VectorClock.Builder clk = getClock();
		RequestMessageProtos.Clock.Builder my_clock = clk.getVectorClockBuilder(MyId);
		int cur_timer = my_clock.getTimer();
		my_clock.setTimer(cur_timer+1);
		return my_clock;
	}
	
	public static void Update_file(String line, int id, RequestMessageProtos.Clock clock){
		// append the counter value to outputfile
		String str_pid = Integer.toString(clock.getPid());
		String str_timer = Integer.toString(clock.getTimer());
		int cnt = ParseOutputFile(line);
		cnt++;
		String new_line="";
		new_line += Integer.toString(cnt);
		new_line += ":" + Integer.toString(id);
		new_line += ":(" + str_pid + "," + str_timer +")";
		
		try(FileWriter fw = new FileWriter(output_file, true); 
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter out = new PrintWriter(bw))
		{
			out.println(new_line);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static RequestMessageProtos.VectorClock create_message(){

		RequestMessageProtos.VectorClock.Builder vclk = getClock();
		RequestMessageProtos.Clock.Builder clk;
		clk = increment_clock();
		vclk.setVectorClock(MyId, clk);
		setClock(vclk);
		return getClock().build();
	}

	public static java.nio.channels.FileLock lock_impl() throws NotBoundException, IOException, InterruptedException{
		RequestMessageProtos.VectorClock.Builder vclk = getClock();
		//System.out.println(vclk.build());
		RequestMessageProtos.Clock.Builder my_clk;
		RequestMessageProtos.VectorClock message;
		
		// Lockrequest
		ack_flag[MyId] = true;
		
		AddQueue(vclk.getVectorClock(MyId));
		for(int i = 1;i <= Nodes;i++){
			
			if(i!=MyId){
				//  handle wait
				 String p = "MyProg-" + Integer.toString(i);
				// 
				 
				 // create message and send request
				 message =  create_message();
				 //adding myself to queue
				
				 System.out.print(Integer.toString(MyId) + "sending lock request to.. " + p + " at ");
				 System.out.println(clockToList(getClock().build()));
				 Request stub = (Request)Naming.lookup(p);
				 RequestMessageProtos.VectorClock lock_req_clock = getClock().build();
				 // receive ack
				 com.google.protobuf.ByteString ack = stub.LockRequest(message.toByteString());
				 RequestMessageProtos.Acknowledgement ack_response = RequestMessageProtos.Acknowledgement.parseFrom(ack);
				 RequestMessageProtos.VectorClock sender_clock = ack_response.getSenderClock();
				 
				 /*increment yours*/
				 RequestMessageProtos.Clock.Builder clk;
				 clk = increment_clock();
				 vclk.setVectorClock(MyId, clk);
				 setClock(vclk);
				 
				 
				 UpdateMyClock(getClock(), sender_clock);
				 if( ack_response.getOk() ){
					 System.out.print(Integer.toString(MyId) + "received ok ack from " + p + " for lock_req ");
					 System.out.println(clockToList(lock_req_clock));
					 ack_flag[i] = true;
				 }else {
					 System.out.print(Integer.toString(MyId) + "received queued ok ack from " + p + " for lock_req ");
					 System.out.println(clockToList(lock_req_clock));
					 ack_flag[i] = false;
				 }
			}
		
		}
		int waiting = 0;
		while(!check_all_acks(ack_flag) || (q.size()>0 && q.get(0).getPid() != MyId  )){
			if(waiting == 0){
				waiting = 1;
				System.out.println( Integer.toString(MyId) + " waiting for lock"  );
				System.out.println("waiting from .. " + clockToList(getClock().build()));
				System.out.println("q elements " + getQElements());
			}
				
			WantLock=true;
			TimeUnit.SECONDS.sleep(1);
		}
		if( check_all_acks(ack_flag) && (q.size()>0 && q.get(0).getPid() == MyId  ) ){
			
			try{
				@SuppressWarnings("resource")
				RandomAccessFile raf = new RandomAccessFile( output_file, "rw" );
				java.nio.channels.FileLock lock =  raf.getChannel().lock();
				System.out.print("lock acquired by: " + Integer.toString(MyId) + " at " );
				System.out.println(clockToList(getClock().build()));
				return lock;
			}catch(Exception e){
				e.printStackTrace();
			}		
		}
		
		return null;
		
	}
	
	public static void unlock(java.nio.channels.FileLock lock ){
		
		RequestMessageProtos.VectorClock message;
		if(lock !=null ){
			try{
				lock.release();
				WantLock=false;
				LockFlag=false;
			}catch(Exception e){
				e.printStackTrace();
			}
			try{
				/*if(q.size() >0 && q.get(0).getPid() == MyId){
					q.remove(0);
				}
				for(int i = 1;i <= Nodes;i++){
					String p = "MyProg-" + Integer.toString(i);
					RequestMessageProtos.Clock.Builder my_vclk = RequestMessageProtos.Clock.newBuilder();
					
					//System.out.println(p);
					if(i!=MyId){
						//  handle wait
						message =  create_message();
						 Request stub = (Request)Naming.lookup(p);
						 System.out.print(Integer.toString(MyId) + " sending unlock request to " + p + " at ");
						 System.out.println(clockToList(getClock().build()));
						 stub.UnlockRequest(message.toByteString());
						 
					}
				}*/
				q.remove(0);
				for(int i = 0;i<q.size();i++){
					String p = "MyProg-" + Integer.toString(q.get(i).getPid());
					message =  create_message();
					Request stub = (Request)Naming.lookup(p);
					System.out.print(Integer.toString(MyId) + " sending unlock request to " + p + " at ");
					System.out.println(clockToList(getClock().build()));
					stub.UnlockRequest(message.toByteString());
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	

	
	public static void main(String args[]) throws FileNotFoundException{
		Nodes = Integer.parseInt(args[0]);
		output_file = args[1];
		MyId = Integer.parseInt(args[2]);
		String s;
		// inserting an initial value of clock into the vector clock to avoid zero-indexing
		RequestMessageProtos.Clock.Builder clock = RequestMessageProtos.Clock.newBuilder() ;
		clock.setPid(0);
		clock.setTimer(0);
		vector_clock.setRpid(MyId);
		vector_clock.addVectorClock(clock);
		
		// initialize the vector clock
		for(int i = 1; i<=Nodes;i++){
		//	clock = RequestMessageProtos.Clock.newBuilder() ;
			clock.setPid(i);
			clock.setTimer(0);
			vector_clock.addVectorClock(clock);
		}
		
		fd = new FileInputStream(new File(output_file));
		// binding name - portmapper
		try{			
			s = "MyProg-";
			s += Integer.toString(MyId);
			MyProg obj = new MyProg();
			int port=PORT + MyId;
			Request req_skel = (Request) UnicastRemoteObject.exportObject(obj, port);
			//Registry registry = LocateRegistry.getRegistry();
			Naming.rebind(s, req_skel);
			//registry.bind(s,  req_skel);
			System.out.println("Bound to " + s);
			System.out.println("Sleeping re... ");
			TimeUnit.SECONDS.sleep(20);
			System.out.println("Woke up re .. ");
		}catch(Exception e){
			e.printStackTrace();
		}
		
		int i = 0;
		java.nio.channels.FileLock lock = null;
		String invoke_com = "";
		while(i<10){
			i++;
			invoke_com = "MyProg -i " + Integer.toString(MyId);
			invoke_com += " -n " + Integer.toString(Nodes);
			invoke_com += " -o " + output_file;
			System.out.print("invoking .. " + Integer.toString(MyId) + " at ");
			System.out.println(clockToList(getClock().build()));
			System.out.println(invoke_com);
			
			try{
				 lock = lock_impl();
			}catch(Exception e){
				e.printStackTrace();
			}
			// critical section code
			if( lock!=null){
				LockFlag=true;
				WantLock=false;
				try{
					BufferedReader reader = new BufferedReader( new InputStreamReader(fd) );
					String last_line = "", current_line;
					while( (current_line = reader.readLine())!= null ){
						last_line = current_line;
					}
					//Object counter = ParseOutputFile(last_line);
					RequestMessageProtos.VectorClock.Builder vclk = getClock();
					RequestMessageProtos.Clock my_clk = vclk.getVectorClock(MyId);
					System.out.print(Integer.toString(MyId) + " updating file at ");
					System.out.println(clockToList(vclk.build()));
					Update_file(last_line, MyId, my_clk);
				}catch(Exception e){
					e.printStackTrace();
				}
				System.out.print(Integer.toString(MyId) + " unlocking at " );
				System.out.println(clockToList(getClock().build()));
				unlock(lock);
			}
			
		}
		
	}
	
	static int Nodes;
	static boolean LockFlag=false;
	static boolean WantLock=false;
	static String output_file;
	static boolean[] ack_flag = new boolean[1001];
	static InputStream fd;
	static RequestMessageProtos.VectorClock.Builder vector_clock = RequestMessageProtos.VectorClock.newBuilder() ;
	static int MyId;
	static int PORT = 10000;
	static LinkedList<RequestMessageProtos.Clock> q=new LinkedList<RequestMessageProtos.Clock>();  
	
	
}
