/**
 * @author wlapka
 *
 * @created May 8, 2014 7:02:18 PM
 */
package net.thoiry.lapka.correlationidentifier;

/**
 * @author wlapka
 *
 */
public class TestInner {
	
	private int x = 5;
	
	public void test() {
		Ala ala = new Ala();
		ala.test();
	}
	
	public void testLocalClass() {
		class LocalClass {
		
			public void test() {
				System.out.println("Jestem w local class");
			}
		}
		LocalClass l = new LocalClass();
		l.test();
	}
	
	private class Ala {
		
		public void test() {
			System.out.println(TestInner.this.x);
		}
		
		
	}
	
	public static void main(String[] args) {
		TestInner t = new TestInner();
		t.test();
		System.out.println("Ala ma kota");
		t.testLocalClass();
	}

}
