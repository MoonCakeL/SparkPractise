package spark.SparkStreaming.transformAtion.window;

/**
 * 每隔几秒，去统计一下最近几秒的数据
 * "每隔几秒"：滑动间隔
 * "最近几秒"：窗口长度
 *
 * "每隔2秒，去处理（聚合）一下最近3秒的RDD数据，然后对这3秒内的RDD进行算法（方法/函数）计算"
 *
 * SparkStreaming提供了滑动窗口操作的支持，从而让我们可以对一个窗口内的数据执行
 * 计算操作，每次掉落在窗口内的RDD数据，会被聚合起来执行计算操作，然后生成的RD，会作为下
 * window DStream的一个RD。
 *
 * 比如图：streaming-dstream-window.png中；
 * 		就是每三秒中的数据执行一次滑动窗口计算，这三秒内的3个RDD会被聚合起来进行处理，然后过了
 * 		两秒，又会对最近三秒的数据执行滑动窗口计算，所有每一个滑动窗口，都必须指定两个参数；
 * 	window length（窗口长度） - 窗口的持续时间；
 * 	sliding interval（滑动间隔） - 执行窗口操作的间隔
 * 这两个参数的长度都必须是source DStream每个batc间隔（batch duration批处理间隔）的整数倍
 *
 * 让我们举例以说明窗口操作. 例如，你想扩展前面的例子用来计算过去 30 秒的词频，间隔时间是 10 秒.
 * 为了达到这个目的，我们必须在过去 30 秒的 (word, 1) pairs 的 pairs DStream 上应用 reduceByKey 操作.
 * 用方法 reduceByKeyAndWindow 实现.
 *
 *  // Reduce last 30 seconds of data, every 10 seconds
	 *		JavaPairDStream<String, Integer> windowedWordCounts =
	 *				pairs.reduceByKeyAndWindow((i1, i2) -> i1 + i2,
	 *											Durations.seconds(30),
	 *											Durations.seconds(10));
 */

public class window {
	/*
	一些常用的窗口操作如下所示，这些操作都需要用到上文提到的两个参数
	- windowLength（窗口长度） 和 slideInterval（滑动的时间间隔）.

	Transformation（转换）								Meaning（含义）
	window(windowLength, slideInterval)					返回一个新的 DStream, 它是基于 source DStream 的窗口
														batch 进行计算的.

	countByWindow(windowLength, slideInterval)			返回 stream（流）中滑动窗口元素的数

	reduceByWindow(func, windowLength, slideInterval)	返回一个新的单元素 stream（流），
														它通过在一个滑动间隔的 stream 中使用 func 来聚合以创建.
														该函数应该是 associative（关联的）且 commutative（
														可交换的），以便它可以并行计算

	reduceByKeyAndWindow
		(func, windowLength, slideInterval, [numTasks])	在一个 (K, V) pairs 的 DStream 上调用时, 返回一个新的
														(K, V) pairs 的 Stream, 其中的每个 key 的 values 是
														在滑动窗口上的 batch 使用给定的函数 func 来聚合产生的.
														Note（注意）: 默认情况下, 该操作使用 Spark 的默认并行任务数量
														（local model 是 2, 在 cluster mode 中的数量
														通过 spark.default.parallelism 来确定）来做 grouping.
														您可以通过一个可选的 numTasks 参数来设置一个不同的 tasks（任务）数量.

	reduceByKeyAndWindow
	(func, invFunc, windowLength, slideInterval, [numTasks]) 与上述 reduceByKeyAndWindow() 的更有效的一个版本，
															其中使用前一窗口的 reduce 值逐渐计算每个窗口的 reduce值.
															这是通过减少进入滑动窗口的新数据，以及 “inverse reducing（逆减）”
															离开窗口的旧数据来完成的.
															一个例子是当窗口滑动时”添加” 和 “减” keys 的数量. 然而，
															它仅适用于 “invertible reduce functions（可逆减少函数）”，
															即具有相应 “inverse reduce（反向减少）” 函数的 reduce 函数
															（作为参数 invFunc </ i>）. 像在 reduceByKeyAndWindow 中的那样,
															reduce 任务的数量可以通过可选参数进行配置.
															请注意, 针对该操作的使用必须启用 checkpointing.

	countByValueAndWindow
	(windowLength, slideInterval, [numTasks])				在一个 (K, V) pairs 的 DStream 上调用时,
															返回一个新的 (K, Long) pairs 的 DStream,
															其中每个 key 的 value 是它在一个滑动窗口之内的频次.
															像 code>reduceByKeyAndWindow</code> 中的那样,
															reduce 任务的数量可以通过可选参数进行配置.
	 */
}
