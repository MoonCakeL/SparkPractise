package JavaBase;

public class JavaBaseTypeAndReferenceType {

	/*Java基本类型和引用类型
	8种基本类型

	一、4种整型
	byte      1字节           -128——127

	short     2 字节         -32,768 —— 32,767

	int       4 字节          -2,147,483,648 ——2,147,483,647（超过20亿）

	long      8 字节   -9,223,372,036,854,775,808——9,223,372,036854,775,807

	注释：java中所有的数据类所占据的字节数量与平台无关，java也没有任何无符号类型

	二、 2种浮点类型
	float    4 字节         32位IEEE 754单精度（有效位数 6 – 7位）

	double   8 字节         64位IEEE 754双精度（有效位数15位）

	三、1种Unicode编码的字符单元
	char    2 字节          整个Unicode字符集

	四、1种真值类型
	boolean    1 位             True或者false

			3种引用类型

			类class

			接口interface

			数组array*/

	public static void main(String[] args){
		int a =10;
		int b =10;

		System.out.println(a==b); //true

		String a1 = "a";
		String a2 = "a";

		System.out.println(a1==a2);	//true

		String x1 = "abc";
		String x2 = "abc";

		System.out.println(x1==x2);	//true

		//"=="是对于基本类型中引用内存中，相同基本类型，的内存地址进行比较

		String b1 = new String("abc");
		String b2 = new String("abc");

		System.out.println(b1==b2);	//false
		System.out.println(b1.equals(b2));	//true equals才是对对象内容的比较
		/*“==”是对对象内存地址的比较
		equals()是对对象内容的比较
		对于基本数据类型一般用“==”
		对于字符串的比较一般用equals()*/
	}
}
