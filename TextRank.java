package de.tuberlin.dima.lyc.algorithm;

import de.tuberlin.dima.lyc.Controller;
import de.tuberlin.dima.lyc.PrecisionStats;
import de.tuberlin.dima.lyc.models.*;
import de.tuberlin.dima.lyc.similarity.CosineSimilarity;
import de.tuberlin.dima.lyc.similarity.EuclideanDistance;
import de.tuberlin.dima.lyc.similarity.SimilarityAlgorithm;
import de.tuberlin.dima.lyc.utils.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

/**
 */
public class TextRank implements Serializable {

	private Scene scene;

	/**
	 * 用FLINK 实现的算法流程：
	 * 1. 将tweets 加载为termsDataSet。(Term, Tweet ID, Number)
	 * 2.
	 *
	 * @param args
	 */
	public static void main(String... args) {
		final TextRank textRank = new TextRank(Controller.config.getCurrentScene());
		//Controller.loadConfig();

		//textRank.generateTweetsKeywords();
		//
		//
		//
		//textRank.generateNewsKeywords();
		//textRank.generateUserKeywords();
		textRank.recommend();
		//textRank.precisionStats();
	}

	public TextRank(Scene scene) {
		this.scene = scene;
	}

	public void generateNewsKeywords() {
		DataSet<NewsModel> newsModelDataSet = NewsModel.loadNewsDataSet(Controller.NEWS.SRC);
		newsModelDataSet.map(new MapFunction<NewsModel, DocumentModel>() {
			@Override
			public DocumentModel map(NewsModel newsModel) throws Exception {
				DocumentModel documentModel = new DocumentModel();
				documentModel.id = newsModel.id;
				documentModel.date = newsModel.date;
				documentModel.keywords = vote(text2wordsSet(newsModel.content, scene.windowSize), scene.numNewsKeywords);
				return documentModel;
			}
		}).writeAsFormattedText(scene.getNewsKeywordsPath(), FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<DocumentModel>() {
			@Override
			public String format(DocumentModel value) {
				return value.toString();
			}
		});
		Controller.run();
	}

	public void generateUserKeywords() {
		String clickPath = Controller.MATCH.RESULT_4COLS;
		int numDays = scene.numDays;
		DataSet<TweetModel> tweetModelDataSet = TweetModel.loadSimpleDataSet(Controller.TWEETS.SRC);

		DataSet<Tuple2<Date, LinkedList<Long>>> uniqueUsers = generateUniqueUsers(clickPath, null);

		DateIterator dateIterator = new DateIterator(scene.fromDate, scene.toDate);
		for (Date date : dateIterator) {
			final DateZoneFilter dateZoneFilter = new DateZoneFilter(date, numDays, 0);

			DataSet<LinkedList<Long>> users = uniqueUsers.reduceGroup(new GroupReduceFunction<Tuple2<Date, LinkedList<Long>>, LinkedList<Long>>() {
				@Override
				public void reduce(Iterable<Tuple2<Date, LinkedList<Long>>> iterable, Collector<LinkedList<Long>> collector) throws Exception {
					LinkedList<Long> users = new LinkedList<Long>();

					for (Tuple2<Date, LinkedList<Long>> input : iterable) {
						if (dateZoneFilter.filter(input.f0)) {
							users.addAll(input.f1);
						}
					}

					collector.collect(users);
				}
			});

			DataSet<DocumentModel> documentDataSet = tweetModelDataSet.cross(users).reduceGroup(new GroupReduceFunction<Tuple2<TweetModel, LinkedList<Long>>, TweetModel>() {
				@Override
				public void reduce(Iterable<Tuple2<TweetModel, LinkedList<Long>>> iterable, Collector<TweetModel> collector) throws Exception {
					for (Tuple2<TweetModel, LinkedList<Long>> input : iterable) {
						if (dateZoneFilter.filter(input.f0.creation_time) && input.f1.contains(input.f0.userId))
							collector.collect(input.f0);
					}
				}
			}).groupBy("userId").reduceGroup(new GroupReduceFunction<TweetModel, DocumentModel>() {
				@Override
				public void reduce(Iterable<TweetModel> iterable, Collector<DocumentModel> collector) throws Exception {
					DocumentModel documentModel = new DocumentModel();
					int count = 0;
					Map<String, Set<String>> words = new HashMap<String, Set<String>>();

					for (TweetModel tweetModel : iterable) {
						if (count == 0) {
							documentModel.userId = tweetModel.userId;
						}
						text2wordsSet(tweetModel.content, words);
					}

					documentModel.keywords = vote(words, scene.numUserKeywords);
					collector.collect(documentModel);
				}
			});

			Controller.log(Controller.language.write_file, scene.getUserKeywordsPath(date));

			documentDataSet.writeAsFormattedText(scene.getUserKeywordsPath(date), FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<DocumentModel>() {
				@Override
				public String format(DocumentModel value) {
					return value.toString();
				}
			});

			Controller.run();
		}
	}


	public Map<String, Set<String>> text2wordsSet(String text, Map<String, Set<String>> words) {
		Tokenizer tokenizer = new Tokenizer();
		Map<String, Integer> tokenList = tokenizer.getTokenMap(text);
		Set<String> keySet = tokenList.keySet();
		if (keySet.size() > 0)
			for (String key : keySet) {
				if (!words.containsKey(key))
					words.put(key, new HashSet<String>());
				Set<String> set = words.get(key);

				for (String value : keySet) {
					if (!key.equals(value)) {
						set.add(value);
					}
				}
			}

		return words;
	}

	public Map<String, Set<String>> text2wordsSet(String text, int windowSize) {
		Tokenizer tokenizer = new Tokenizer();
		List<String> tokenList = tokenizer.getTokenList(text);

		Map<String, Set<String>> words = new HashMap<String, Set<String>>();

		for (int index = 0, left, right, cursor; index < tokenList.size(); index++) {
			String token = tokenList.get(index);
			Set<String> set;
			if (words.containsKey(token))
				set = words.get(token);
			else {
				set = new HashSet<String>();
				words.put(token, set);
			}

			left = index - windowSize;
			if (left < 0) left = 0;

			right = index + windowSize;
			if (right >= tokenList.size()) right = 0;

			for (cursor = left; cursor <= right && cursor != index; cursor++) {
				set.add(tokenList.get(cursor));
			}
		}

		return words;
	}

	public List<Keyword> vote(Map<String, Set<String>> words, int numKeywords) {
		double d = scene.friction;
		Map<String, Double> score = new HashMap<String, Double>();

		for (int i = 0; i < scene.maxIterates; ++i) {
			Map<String, Double> m = new HashMap<String, Double>();
			double max_diff = 0;

			for (Map.Entry<String, Set<String>> entry : words.entrySet()) {
				String word = entry.getKey();
				Set<String> neighbors = entry.getValue();
				m.put(word, 1 - d);
				for (String neighbor : neighbors) {
					int size = words.get(neighbor).size();
					if (word.equals(neighbor) || size == 0) continue;
					m.put(word, m.get(word) + d / size * (score.get(neighbor) == null ? 0 : score.get(neighbor)));
				}
				max_diff = Math.max(max_diff, Math.abs(m.get(word) - (score.get(word) == null ? 0 : score.get(word))));
			}
			score = m;
			if (max_diff <= scene.minDifference) break;
		}

		return Utils.firstN(score, numKeywords);
	}

	void recommend() {
		String clickPath = Controller.MATCH.RESULT_4COLS;
		DataSet<Tuple2<Date, LinkedList<Long>>> uniqueUsers = generateUniqueUsers(clickPath, null);

		final DataSet<DocumentModel> newsKeywords = DocumentModel.loadDocumentDataSet(scene.getNewsKeywordsPath());
		Controller.log("Load news keywords: %s", scene.getNewsKeywordsPath());

		final CosineSimilarity similarity = new CosineSimilarity();
		final EuclideanDistance euclideanDistance = new EuclideanDistance();
		DataSet<TweetModel> tweetModelDataSet = TweetModel.loadSimpleDataSet(Controller.TWEETS.SRC);

		DateIterator dateIterator = new DateIterator(scene.fromDate, scene.toDate);
		for (final Date today : dateIterator) {
			final Date TEMP_DATE = new Date(today.getTime());
			final DateLoopFilter dateLoopFilter = new DateLoopFilter(Controller.GLOBAL.GAP, scene.fromDate, scene.toDate, TEMP_DATE);
			// Filter date here.
			final DateZoneFilter newsDateFilter = new DateZoneFilter(TEMP_DATE, scene.numDays, 1);

			// 将一日的所有用户收集起来。
			DataSet<Tuple2<Date, LinkedList<Long>>> todayUsers = uniqueUsers.filter(new FilterFunction<Tuple2<Date, LinkedList<Long>>>() {
				@Override
				public boolean filter(Tuple2<Date, LinkedList<Long>> input) throws Exception {
					return TEMP_DATE.equals(input.f0);
				}
			});

			DataSet<DocumentModel> userKeywords = DocumentModel.loadDocumentDataSet(scene.getUserKeywordsPath(today)).cross(todayUsers)
					.flatMap(new FlatMapFunction<Tuple2<DocumentModel, Tuple2<Date, LinkedList<Long>>>, DocumentModel>() {
						@Override
						public void flatMap(Tuple2<DocumentModel, Tuple2<Date, LinkedList<Long>>> input, Collector<DocumentModel> collector) throws Exception {
							if (input.f1.f1.contains(input.f0.userId)) {
								collector.collect(input.f0);
							}
						}
					});

			Controller.log("Load user keywords: %s", scene.getUserKeywordsPath(today));

			userKeywords.cross(newsKeywords.filter(new FilterFunction<DocumentModel>() {
				@Override
				public boolean filter(DocumentModel documentModel) throws Exception {
					return newsDateFilter.filter(documentModel.date);
				}
			})).flatMap(new FlatMapFunction<Tuple2<DocumentModel, DocumentModel>, Tuple3<Long, Long, Double>>() {
				@Override
				public void flatMap(Tuple2<DocumentModel, DocumentModel> input, Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
					double value = 0;
					if (scene.similarityAlgorithm == SimilarityAlgorithm.COSINE_SIMILARITY) {
						value = similarity.calculateX(input.f0.keywords, input.f1.keywords);
					} else {
						// Euclidean distance.
						if (input.f0.keywords != null && input.f1.keywords != null)
							value = euclideanDistance.compute(input.f0.keywords, input.f1.keywords);
						else return;
					}
					if (value > 0) {
						//System.out.println(input.f0.userId + ", " + input.f1.id + ", " + value);
						collector.collect(new Tuple3<Long, Long, Double>(input.f0.userId, input.f1.id, value));
					}
				}
			}).groupBy(0).sortGroup(2, Order.DESCENDING).reduceGroup(new GroupReduceFunction<Tuple3<Long, Long, Double>, RecommendationModel>() {
				@Override
				public void reduce(Iterable<Tuple3<Long, Long, Double>> iterable, Collector<RecommendationModel> collector) throws Exception {
					RecommendationModel recommendation = new RecommendationModel();
					boolean first = true;
					int count = 0;
					for (Tuple3<Long, Long, Double> input : iterable) {
						if (first) {
							first = false;
							recommendation.userId = input.f0;
							recommendation.date = TEMP_DATE;
							recommendation.news = new LinkedList<Long>();
						}
						recommendation.news.add(input.f1);
						count++;
						if (count >= scene.numRecommendations)
							break;
					}
					collector.collect(recommendation);
				}
			}).writeAsFormattedText(scene.getRecommendationsPath(today), FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<RecommendationModel>() {
				@Override
				public String format(RecommendationModel value) {
					return value.toString();
				}
			});

			Controller.log(Controller.language.write_file, scene.getRecommendationsPath(today));

			Controller.run();
		}

	}

	/**
	 * 获取每天产生点击的用户的集合。
	 *
	 * @param clickPath
	 * @param outputPath Write to file if outputPath is not null.
	 * @return
	 */
	public DataSet<Tuple2<Date, LinkedList<Long>>> generateUniqueUsers(String clickPath, String outputPath) {
		final DataSet<ClickModel> clickDataSet = ClickModel.loadClickDataSet(clickPath);
		DataSet<Tuple2<Date, LinkedList<Long>>> rtDataSet = clickDataSet.groupBy("date").reduceGroup(new GroupReduceFunction<ClickModel, Tuple2<Date, LinkedList<Long>>>() {
			@Override
			public void reduce(Iterable<ClickModel> iterable, Collector<Tuple2<Date, LinkedList<Long>>> collector) throws Exception {
				Tuple2<Date, LinkedList<Long>> output = new Tuple2<Date, LinkedList<Long>>();
				output.f1 = new LinkedList<Long>();
				boolean first = true;
				for (ClickModel clickModel : iterable) {
					if (first) {
						first = false;
						output.f0 = clickModel.date;
					}
					if (!output.f1.contains(clickModel.userId)) {
						output.f1.add(clickModel.userId);
					}
				}
				collector.collect(output);
			}
		});

		if (outputPath != null)
			rtDataSet.writeAsFormattedText(outputPath, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Tuple2<Date, LinkedList<Long>>>() {

				@Override
				public String format(Tuple2<Date, LinkedList<Long>> value) {
					return DateUtils.date2str(value.f0) + value.toString();
				}
			});

		return rtDataSet;
	}

	public void precisionStats() {
		PrecisionStats precisionStats = new PrecisionStats();

		List<Tuple4<Date, Integer, Integer, Double>> list = new LinkedList<Tuple4<Date, Integer, Integer, Double>>();
		list.add(new Tuple4<Date, Integer, Integer, Double>(new Date(0), 0, 0, 0.0));
		DataSet<Tuple4<Date, Integer, Integer, Double>> allStats = Controller.getEnvironment().fromCollection(list);

		DateIterator dateIterator = new DateIterator(scene.fromDate, scene.toDate);
		for (final Date today : dateIterator) {
			DataSet<StatItem> statItemDataSet = precisionStats.precisions(scene.getRecommendationsPath(today), Controller.MATCH.RESULT_4COLS, scene.getPrecisionsPath(today), today, false);
			precisionStats.statAll(statItemDataSet, today, scene.getStatsPath(today));

			Controller.run();
		}
	}
}
