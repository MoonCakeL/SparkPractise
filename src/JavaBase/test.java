package JavaBase;

public class test {
	public static void main(String[] args){
		String a ="20140401173500";

		//int num = Integer.valueOf(a);
		//java.lang.NumberFormatException: For input string: "20140401173500"
		//System.out.println(num);

		/*public static Integer valueOf(String s) throws NumberFormatException
		{
			return new Integer(parseInt(s, 10));
		}*/

		/*List<String[]> list = new ArrayList<>();
		List<String> list2 = new ArrayList<>();
		String[] strings ={"1"};
		list2.add("1");
		list.add(0,strings);

		System.out.println(strings[0]);
		System.out.println(String.valueOf(list.get(0)));*/

		String[] s = new  String[1];

		/*s[0]="哈哈";
		s[0]="哈哈2";
		s[0]="哈哈3";*/
		s[0]=null;

		System.out.println(s[0]);
	}
}