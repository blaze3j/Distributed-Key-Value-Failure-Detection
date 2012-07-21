package utils;

public class Output {
	
	public synchronized static void println(String str){		
		System.out.println(str);
	}
	
	public synchronized static void print(String str){
		System.out.print(str);
	}
}
