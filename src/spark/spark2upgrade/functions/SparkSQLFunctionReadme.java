package spark.spark2upgrade.functions;

public class SparkSQLFunctionReadme {
	/**
	 * Functions available for DataFrame operations.
	 *
	 * @groupname udf_funcs UDF functions
	 * @groupname agg_funcs Aggregate functions
	 * @groupname datetime_funcs Date time functions
	 * @groupname sort_funcs Sorting functions
	 * @groupname normal_funcs Non-aggregate functions
	 * @groupname math_funcs Math functions
	 * @groupname misc_funcs Misc functions
	 * @groupname window_funcs Window functions
	 * @groupname string_funcs String functions
	 * @groupname collection_funcs Collection functions
	 * @groupname Ungrouped Support functions for DataFrames
	 * @since 1.3.0
	 * 在Spark 1.3.x版本后(1.5.x)，增加了一系列内置函数到DataFrame API中，并且实现了code-generation的优化。
	 * 与普通的函数不同，DataFrame的函数并不会执行后立即返回一个结果值，而是返回一个Column对象，
	 * 用于在并行作业中进行求值。Column可以用在DataFrame的操作之中，比如select，filter，groupBy等。
	 * 函数的输入值，也可以是Column。
	 * 在java中还是基于hive的，通过sql语句中的内置函数，sparksql会自动进行底层内置函数的应用，与scala不一样

	 *1.聚合函数	aggregate function:

	 	approxCountDistinct, avg, count, countDistinct, first, last, max, mean, min, sum, sumDistinct

	 *2.集合函数 set function:

	 	array_contains, explode, size, sort_array

	 *3.日期时间转换 time function

	 	unix_timestamp, from_unixtime, to_date, quarter, day, dayofyear, weekofyear, from_utc_timestamp,
	 	to_utc_timestamp

	 	*从日期时间中提取字段
	 		year, month, dayofmonth, hour, minute, second

	 	*日期/时间计算

	 		datediff, date_add, date_sub, add_months, last_day, next_day, months_between

	 	*获取当前时间等
	 		current_date, current_timestamp, trunc, date_format

	 *4.数学函数 Math; mathematical function; ：

	 		abs, acros, asin, atan, atan2, bin, cbrt, ceil, conv, cos, sosh, exp, expm1, factorial,
	 		floor, hex, hypot, log, log10, log1p, log2, pmod, pow, rint, round, shiftLeft, shiftRight,
	 		shiftRightUnsigned, signum, sin, sinh, sqrt, tan, tanh, toDegrees, toRadians, unhex

	 *5.混合函数:

	 	array, bitwiseNOT, callUDF, coalesce, crc32, greatest, if, inputFileName, isNaN, isnotnull,
	 	isnull, least, lit, md5, monotonicallyIncreasingId, nanvl, negate, not, rand, randn, sha, sha1,
	 	sparkPartitionId, struct, when

	 *6.字符串函数 String function:

	 	ascii, base64, concat, concat_ws, decode, encode, format_number, format_string, get_json_object,
	 	initcap, instr, length, levenshtein, locate, lower, lpad, ltrim, printf, regexp_extract,
	 	regexp_replace, repeat, reverse, rpad, rtrim, soundex, space, split, substring, substring_index, translate, trim, unb

	 *7.窗口函数 window function:

	 	cumeDist, denseRank, lag, lead, ntile, percentRank, rank, rowNumber

	 	ase64, upper
	 * @param args
	 */
	/*
	内置函数源码
		"spark-master/sql/core/src/main/scala/org/apache/spark/sql/functions.scala "
	 */
}
