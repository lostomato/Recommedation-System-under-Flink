package de.tuberlin.dima.lyc.similarity;

import de.tuberlin.dima.lyc.models.Keyword;

import java.io.Serializable;
import java.util.*;

public class CosineSimilarity implements Serializable {

	public double computeBoolean(List<String> keywords1, List<String> keywords2) {
		int capacity = keywords1.size() + keywords2.size();
		List<String> union = new ArrayList<String>(capacity);

		{ // 生成共同数组。
			boolean unique;
			for (String key1 : keywords1) {
				if (key1 != null && !key1.isEmpty()) {
					unique = true;
					for (String key2 : keywords2) {
						if (key2 != null && !key2.isEmpty()) {
							if (key1.equals(key2)) {
								unique = false;
								break;
							}
						}
					}
					if (unique) union.add(key1);
				}
			}
			for (String key2 : keywords2) {
				if (key2 != null && !key2.isEmpty()) {
					union.add(key2);
				}
			}
		}

		boolean[] tuple1 = getTuple(union, keywords1);
		boolean[] tuple2 = getTuple(union, keywords2);

		int numerator = 0;
		int denominator1 = 0;
		int denominator2 = 0;
		for (int i = 0; i < tuple1.length; i++) {
			if (tuple1[i] && tuple2[i]) numerator++;
			if (tuple1[i]) denominator1++;
			if (tuple2[i]) denominator2++;
		}
		return (double) numerator / (denominator1 * denominator2);
	}


	public double compute(List<Keyword> keywords1, List<Keyword> keywords2) {
		HashMap<String, Double> union = new HashMap<String, Double>();
		HashMap<String, Double> intersection = new HashMap<String, Double>();

		for (Keyword key1 : keywords1) {
			union.put(key1.word, key1.value);
		}

		double min, max;
		for (Keyword key2 : keywords2) {
			if (union.containsKey(key2.word)) {
				min = union.get(key2.word); // Value of key1
				max = key2.value;

				if (min > max) {
					max = min;
					min = key2.value; // The min value of key1 and key2, which is also the intersection.
				}

				intersection.put(key2.word, min);
				union.put(key2.word, max);
			} else
				union.put(key2.word, key2.value);
		}

		double numerator = 0, denominator = 0;
		for (Double d : intersection.values()) {
			numerator += d * d;
		}
		for (Double d : union.values()) {
			denominator += d * d;
		}

		return numerator / denominator;
	}

	public double calculateX(List<Keyword> keywords1, List<Keyword> keywords2) {
		int capacity = keywords1.size() + keywords2.size();
		List<String> common = new ArrayList<String>(capacity);

		{ // 生成共同数组。
			boolean unique;
			for (Keyword key1 : keywords1) {
				if (key1 != null) {
					unique = true;
					for (Keyword key2 : keywords2) {
						if (key2 != null) {
							if (key1.word.equals(key2.word)) {
								unique = false;
								break;
							}
						}
					}
					if (unique) common.add(key1.word);
				}
			}
			for (Keyword key2 : keywords2) {
				if (key2 != null) {
					common.add(key2.word);
				}
			}
		}

		boolean[] tuple1 = getTupleX(common, keywords1);
		boolean[] tuple2 = getTupleX(common, keywords2);

		int numerator = 0;
		int denominator1 = 0;
		int denominator2 = 0;
		for (int i = 0; i < tuple1.length; i++) {
			if (tuple1[i] && tuple2[i]) numerator++;
			if (tuple1[i]) denominator1++;
			if (tuple2[i]) denominator2++;
		}
		return (double) numerator / (denominator1 * denominator2);
	}

	/**
	 * 计算keywords 和 并集之间的关系，相交的word 视为true，否则视为false。
	 *
	 * @param common   所有keywords 的集合
	 * @param keywords 一组keywords
	 * @return
	 */
	private boolean[] getTuple(List<String> common, Collection<String> keywords) {
		int size = common.size();
		boolean[] tuple = new boolean[size];
		String key;
		boolean same;

		loop1:
		for (int cursor = 0; cursor < size; cursor++) {
			key = common.get(cursor);
			same = false;
			for (String word : keywords) {
				if (key.equals(word)) {
					same = true;
					break;
				}
			}
			tuple[cursor] = same;
		}
		return tuple;
	}

	private boolean[] getTupleX(List<String> common, Collection<Keyword> keywords) {
		int size = common.size();
		boolean[] tuple = new boolean[size];
		String key;
		boolean same;

		loop1:
		for (int cursor = 0; cursor < size; cursor++) {
			key = common.get(cursor);
			same = false;
			for (Keyword word : keywords) {
				if (key.equals(word.word)) {
					same = true;
					break;
				}
			}
			tuple[cursor] = same;
		}
		return tuple;
	}
}
