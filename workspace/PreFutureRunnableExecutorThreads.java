package com.poc.rcm.java.eight;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PreFutureRunnableExecutorThreads {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("printing current thread name and details : "+Thread.currentThread().getThreadGroup()+ " : thread name : " +Thread.currentThread().getName());

		
		ExecutorService service = Executors.newFixedThreadPool(3);
		
		for(int i=0;i<10;i++){
			service.execute(new Task());
		}
		
		System.out.println("printing current thread name and details : "+Thread.currentThread().getThreadGroup()+ " : thread name : " +Thread.currentThread().getName());

	}
	
	public static class Task implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			Random rand = new Random();
			
			System.out.println("printing current thread name and details : "+Thread.currentThread().getThreadGroup()+ " : thread name : " +Thread.currentThread().getName());

			System.out.println("printing the randomly generated number : "+rand.nextInt());
		}
		
		
		
	}
}

/*
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-1
printing the randomly generated number : 321651693
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-1
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
printing the randomly generated number : -496802420
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
printing the randomly generated number : -674106159
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
printing the randomly generated number : -2015170626
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-3
printing the randomly generated number : 1600242591
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
printing the randomly generated number : -1474974303
printing the randomly generated number : 1175800234
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
printing the randomly generated number : 151069011
printing the randomly generated number : 549058605
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-1
printing the randomly generated number : -359713987

*/