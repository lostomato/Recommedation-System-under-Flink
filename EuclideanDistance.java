package de.tuberlin.dima.lyc.similarity;

import de.tuberlin.dima.lyc.models.Keyword;
import de.tuberlin.dima.lyc.utils.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.security.Key;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 */
public class EuclideanDistance implements Serializable {

	/**
	 * Float point version of EuclideanDistance.
	 *
	 * @param keywords1
	 * @param keywords2
	 * @return
	 */
	public double compute(List<Keyword> keywords1, List<Keyword> keywords2) {
		HashMap<String, Double> map = new HashMap<String, Double>();
		for (Keyword keyword : keywords1) {
			map.put(keyword.word, keyword.value);
		}

		double euclideanDistance = 0;
		for (Keyword keyword : keywords2) {
			if (map.containsKey(keyword.word)) {
				euclideanDistance += map.get(keyword.word) * keyword.value;
			}
		}

		return euclideanDistance;
	}

	/**
	 * Boolean version of EuclideanDistance.
	 *
	 * @param keywords1
	 * @param keywords2
	 * @return
	 */
	public int calculate(String[] keywords1, String[] keywords2) {
		int distance = 0;
		int length1 = keywords1.length;
		int length2 = keywords2.length;

		HashMap<String, Integer> map2 = new HashMap<String, Integer>();
		int multiple = length1 * length2;
		for (String s2 : keywords2) {
			map2.put(s2, multiple);
			multiple -= length1;
		}

		multiple = length1 * length2;
		int temp;
		for (String s1 : keywords1) {
			if (map2.containsKey(s1)) {
				temp = multiple - map2.get(s1);
				map2.put(s1, 0);
			} else {
				temp = multiple;
			}
			distance += temp * temp;
			multiple -= length2;
		}

		for (String key : map2.keySet()) {
			temp = map2.get(key);
			distance += temp * temp;
		}

		return distance;
	}

	/**
	 * 现在开始计算正向值。
	 *
	 * @param keywords1
	 * @param keywords2
	 * @param length1
	 * @param length2
	 * @return
	 */
	public int calculate(String[] keywords1, String[] keywords2, int length1, int length2) {
		int distance = 0;
		int multiple1 = length1 * length2;
		int multiple2;

		for (String key1 : keywords1) {
			if (key1 != null) {
				multiple2 = length1 * length2;
				for (String key2 : keywords2) {
					if (key1.equals(key2)) {
						distance += multiple1 * multiple2;
					}
					multiple2 -= length1;
				}
			}
			multiple1 -= length2;
		}
		return distance;
	}

	/**
	 * 为了加快运算速度，keywords1 的数据量要大于keywords2 的数据量。
	 *
	 * @param keywords1
	 * @param keywords2
	 * @return
	 */
	public int calculate(LinkedList<Tuple2<String, Double>> keywords1, LinkedList<Tuple2<String, Double>> keywords2) {
		int distance = 0;
		int length1 = keywords1.size();
		int length2 = keywords2.size();

		HashMap<String, Integer> map2 = new HashMap<String, Integer>();
		int multiple = length1 * length2;
		for (Tuple2<String, Double> t2 : keywords2) {
			map2.put(t2.f0, multiple);
			multiple -= length1;
		}

		multiple = length1 * length2;
		int temp;
		for (Tuple2<String, Double> t1 : keywords1) {
			if (map2.containsKey(t1.f0)) {
				temp = multiple - map2.get(t1.f0);
				map2.put(t1.f0, 0);
			} else {
				temp = multiple;
			}
			distance += temp * temp;
			multiple -= length2;
		}

		for (String key : map2.keySet()) {
			temp = map2.get(key);
			distance += temp * temp;
		}

		return distance;
	}

	public int calculateMax(int length1, int length2) {
		int value = 0;
		for (int multiple = length1 * length2; multiple > 0; multiple -= length1) {
			value += multiple * multiple;
		}

		for (int multiple = length1 * length2; multiple > 0; multiple -= length2) {
			value += multiple * multiple;
		}
		return value;
	}
}
