package spark.core.Demo;

import scala.Serializable;
import scala.math.Ordered;

import java.util.Objects;

		/*1,5
		2,4
		3,6
		1,3
		2,1*/
/**
 * //实现一个二次排序的功能
 //如 先按照第一列比较大小， 如果第一列相同 就 比较第二列的大小进行排序
 //再spark中进行二次排序，首先需要自己定义一个funtion的自定义排序key
 //需要实现"scala.math"中提供的，Ordered这个接口以及"scala"中提供的序列化接口Serializable
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {
	private static final long serialVersionUID = 1L;
	//1.首先再自定义的key里面，定义需要进行先后进入方法内前后两个key排序的列,通过两个列的大小进行比较

	//定义两个first私有遍历，用于当地一个key相等是，通过第二个key判断大小
	private int first;
	private int second;

	public SecondarySortKey(int first, int second) {
		this.first = first;
		this.second = second;
	}

	//直接Command+N 调出Ordered接口中需要实现的方法，通过比较出大小后，利用方法进行逻辑判断：如下
	@Override
	//compare()用来对排序的类进行比较，实际上使用的待比较的对象和compareTo()一样，所以逻辑相同
	public int compare(SecondarySortKey other) {
		//如果先进来的fist key与后进来的first key不相等（相减不等于0）
		if(this.first - other.getFirst() !=0){
			//那么就返回先进来的fist key与后进来的first key相减的值
			//-1为小于；1为大于
			return this.first - other.getFirst();
			//如果如果先进来的fist key与后进来的first key相等（相减等于0）
		}else if (this.first - other.getFirst() ==0){
			//那么进行第二列的比较，相减返回值
			//-1为小于；1为大于
			return this.second - other.getSecond();
		}
		//0为等于
		return 0;
	}

	@Override
	//小于：给出小于的判断逻辑
	public boolean $less(SecondarySortKey other) {
		//如果我的当前的first小于后进来的first，那就返回true
		if (this.first < other.getFirst()){
			return true;
			//第二层判断逻辑，如果当前的first等于后进来的first，那么进行第二列的second判断
			//如果当前的第二列，小于，后进来的第二列，那么就是true
		}else if (this.first == other.getFirst() && this.second < other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	//$greater方法 大于：定义当前key判断大于后进来的key的逻辑
	public boolean $greater(SecondarySortKey other) {
		//第一层逻辑判断：如果当前的first key大于后进来的first key 自然就返回true
		if (this.first > other.getFirst()){
			return true;
			//第二层逻辑判断：如果当前的first key等于后进来的first key，就对后进来行的第二个key进行比较
			//			   如果当前的first key，大于，后进来的first key，的第二个second key就返回true
		}else if (this.first == other.getFirst() && this.second > other.getSecond())
		{
			return true;
		}
		return false;
	}

	@Override
	//小于等于，给出小于等于的判断逻辑
	public boolean $less$eq(SecondarySortKey other) {
		//第一层如果当前的first key小于等于后进来的first key那么就返回true
		if (this.first < other.getFirst()){
			return true;
			//第二层逻辑 如果当前的first key等于后进来的first key，那么就进行比较第二个值
			//如果当前first key的第二值，等于，后进来的first key的第二个值 那么就返回true
		}else if (this.first == other.getFirst() && this.second == other.getSecond()){
			return true;
		}

		return false;
	}

	@Override
	//大于等于，给出大于等于的判断逻辑
	public boolean $greater$eq(SecondarySortKey other) {
		//如果当前的first key大于后进来的first key那就返回true
		if (this.first > other.getFirst()){
			return true;
			//第二层判断逻辑，如果当前first key等于后进来的first key，那么就比较第二个值
			//如果当前first key的第二值，等于，后进来firstkey的第二个值，那么就返回true
		}else if (this.first == other.getFirst() && this.second == other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public int compareTo(SecondarySortKey other) {
		//如果先进来的fist key与后进来的first key不相等（相减不等于0）
		if(this.first - other.getFirst() !=0){
			//那么就返回先进来的fist key与后进来的first key相减的值
			//-1为小于；1为大于
			return this.first - other.getFirst();
			//如果如果先进来的fist key与后进来的first key相等（相减等于0）
		}else if (this.first - other.getFirst() ==0){
			//那么进行第二列的比较，相减返回值
			//-1为小于；1为大于
			return this.second - other.getSecond();
		}
		//0为等于
		return 0;
	}

	//2.为将要传入进来的列提供getter和setter方法，以及hashCode方法和equals方法（比较大小）
	//直接Command+N调出

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SecondarySortKey that = (SecondarySortKey) o;
		return first == that.first &&
				second == that.second;
	}


	@Override
	public int hashCode() {

		return Objects.hash(first, second);
	}
}

