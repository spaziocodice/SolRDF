package org.gazzax.labs.solrdf.handler.search.algebra;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

//import static java.util.Comparator.*;
import static java.util.stream.Collectors.toList;

public class Main {
	public static void main(String[] args) {
		List<Integer> s = new ArrayList<Integer>();
		Random r = new Random();
		for (int i = 0; i < 10000000; i++) {
			s.add(r.nextInt(100));
		}
		
		List<Integer> os = s
				.parallelStream()
				.map(i -> { 
//					System.out.println(Thread.currentThread());
					return i + 1;
				
				})
				.sorted()
				.collect(toList());
		
		for (Integer i : os) {
			System.out.println(i);
		}
		
	}
	
}
