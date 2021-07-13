package com.poc.rcm.java.eight;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FutureCallableExecutorThreads {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		System.out.println("printing current thread name and details : "+Thread.currentThread().getThreadGroup()+ " : thread name : " +Thread.currentThread().getName());

		
		ExecutorService service = Executors.newFixedThreadPool(3);
		
		for(int i=0;i<10;i++){
			Future<Integer> placeHolderValues = service.submit(new Task());
			System.out.println(placeHolderValues.get().intValue());
		}
		
		for(int i=0;i<100;i++){
			System.out.println(i);
			System.out.println("printing current thread name and details : "+Thread.currentThread().getThreadGroup()+ " : thread name : " +Thread.currentThread().getName());
		}
	}
	
	public static  class Task implements Callable{

		@Override
		public Integer call() throws Exception {
			// TODO Auto-generated method stub
			System.out.println("printing current thread name and details : "+Thread.currentThread().getThreadGroup()+ " : thread name : " +Thread.currentThread().getName());
			return new Random().nextInt();
		}

		
		
		
		
	}
}


/*printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-1
1623801403
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
236887764
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-3
1910690801
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-1
-1967253097
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
-947895841
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-3
1377956061
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-1
-1135937698
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-2
1990231876
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-3
401828207
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : pool-1-thread-1
-2132984269
0
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
1
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
2
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
3
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
4
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
5
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
6
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
7
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
8
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
9
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
10
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
11
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
12
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
13
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
14
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
15
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
16
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
17
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
18
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
19
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
20
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
21
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
22
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
23
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
24
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
25
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
26
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
27
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
28
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
29
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
30
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
31
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
32
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
33
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
34
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
35
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
36
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
37
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
38
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
39
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
40
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
41
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
42
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
43
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
44
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
45
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
46
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
47
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
48
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
49
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
50
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
51
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
52
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
53
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
54
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
55
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
56
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
57
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
58
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
59
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
60
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
61
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
62
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
63
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
64
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
65
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
66
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
67
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
68
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
69
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
70
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
71
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
72
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
73
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
74
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
75
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
76
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
77
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
78
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
79
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
80
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
81
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
82
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
83
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
84
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
85
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
86
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
87
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
88
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
89
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
90
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
91
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
92
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
93
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
94
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
95
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
96
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
97
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
98
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
99
printing current thread name and details : java.lang.ThreadGroup[name=main,maxpri=10] : thread name : main
*/
