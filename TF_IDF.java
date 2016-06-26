package de.tuberlin.dima.lyc.algorithm;

import de.tuberlin.dima.lyc.Controller;
import de.tuberlin.dima.lyc.PrecisionStats;
import de.tuberlin.dima.lyc.models.*;
import de.tuberlin.dima.lyc.similarity.CosineSimilarity;
import de.tuberlin.dima.lyc.similarity.EuclideanDistance;
import de.tuberlin.dima.lyc.similarity.SimilarityAlgorithm;
import de.tuberlin.dima.lyc.utils.DateIterator;
import de.tuberlin.dima.lyc.utils.DateUtils;
import de.tuberlin.dima.lyc.utils.DateLoopFilter;
import de.tuberlin.dima.lyc.utils.DateZoneFilter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class TF_IDF implements Serializable{

	public static void main(String... args) {
		TF_IDF tf_idf = new TF_IDF(Controller.config.getCurrentScene());
		//tf_idf.generateNewsKeywords(Controller.NEWS.KEYWORDS);
		//generator.generateTweetsKeywords();
		//generator.generateUserKeywords();
		//generator.recommend(SimilarityAlgorithm.COSINE_SIMILARITY);

		//generator.generateDailyUserKeywords();
		//generator.generateUniqueUsers(Controller.MATCH.RESULT_4COLS, Controller.MATCH.UNIQUE_USERS);
		//Controller.run();

		tf_idf.recommend();
		//tf_idf.precisionStats();
	}

	private Scene scene;

	public TF_IDF(Scene scene){
		this.scene = scene;
	}

	void generateNewsKeywords(String outputPath) {
		String csvPath = Controller.NEWS.SRC;//_25ROWS;
		float titleWeight = Controller.NEWS.TITLE_WEIGHT;
		float descriptionWeight = Controller.NEWS.DESCRIPTION_WEIGHT;
		float contentWeight = Controller.NEWS.CONTENT_WEIGHT;
		int numNews = Controller.GLOBAL.NUM_NEWS;
		int numKeywords = Controller.GLOBAL.NUM_NEWS_KEYWORDS;

		DataSet<NewsModel> srcDataSet = NewsModel.loadNewsDataSet(csvPath);
		DataSet<TermModel> termDataSet = NewsModel.generateTermDataSet(srcDataSet, titleWeight, descriptionWeight, contentWeight);
		termDataSet = TermModel.countTFIDF(termDataSet, numNews);
		/*termDataSet.writeAsFormattedText(Controller.NEWS.TERMS_STATS, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<TermModel>() {
			@Override
			public String format(TermModel value) {
				return value.toString();
			}
		});*/

		DataSet<DocumentModel> documentDataSet = DocumentModel.fromTermDataSet(termDataSet, numKeywords);

		documentDataSet.writeAsFormattedText(outputPath, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<DocumentModel>() {
			@Override
			public String format(DocumentModel value) {
				return value.toString();
			}
		});

		Controller.run();
	}

	void generateTweetsKeywords() {
		String csvPath = Controller.TWEETS.SRC;// _25ROWS;
		float newsWeight = Controller.TWEETS.NEWS_WEIGHT;
		float tweetWeight = Controller.TWEETS.TWEET_WEIGHT;
		int numTweets = Controller.TWEETS.NUM_TWEETS;
		int numKeywords = Controller.GLOBAL.NUM_NEWS_KEYWORDS;

		DataSet<TweetModel> tweetsDataSet = TweetModel.loadSimpleDataSet(csvPath);
		DataSet<TermModel> termDataSet = TweetModel.generateTermDataSet(tweetsDataSet);
		termDataSet = TermModel.countTFIDF(termDataSet, numTweets);

		DataSet<DocumentModel> documentDataSet = DocumentModel.fromTermDataSet(termDataSet, numKeywords);

		documentDataSet.writeAsFormattedText(Controller.TWEETS.KEYWORDS, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<DocumentModel>() {
			@Override
			public String format(DocumentModel value) {
				return value.toString();
			}
		});
		Controller.run();
	}

	private void generateUserKeywords() {
		DataSet<DocumentModel> documentDataSet = DocumentModel.loadDocumentDataSet(Controller.TWEETS.KEYWORDS);
		DataSet<DocumentModel> userKeywords = DocumentModel.generateUserKeywords(documentDataSet, Controller.GLOBAL.NUM_USER_KEYWORDS);

		userKeywords.writeAsFormattedText(Controller.TWEETS.USER_KEYWORDS_SIMPLE, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<DocumentModel>() {
			@Override
			public String format(DocumentModel value) {
				return value.toString();
			}
		});
		Controller.run();
	}

	private void generateDailyUserKeywords() {
		Date fromDate = Controller.GLOBAL.FROM_DATE;
		final Date toDate = Controller.GLOBAL.TO_DATE;
		Date today = new Date(fromDate.getTime());
		String tweetsKeywordsPath = Controller.TWEETS.KEYWORDS;

		DataSet<DocumentModel> tweetsKeywords = DocumentModel.loadDocumentDataSet(tweetsKeywordsPath);

		do {
			final Date TEMP_DATE = new Date(today.getTime());
			final DateLoopFilter dateLoopFilter = new DateLoopFilter(Controller.GLOBAL.GAP, fromDate, toDate, TEMP_DATE);

			DataSet<DocumentModel> userKeywords = DocumentModel.generateUserKeywords(tweetsKeywords.filter(new FilterFunction<DocumentModel>() {
				@Override
				public boolean filter(DocumentModel document) throws Exception {
					return dateLoopFilter.filter(document.date);
				}
			}), Controller.GLOBAL.NUM_USER_KEYWORDS);
			userKeywords.writeAsFormattedText(Controller.TWEETS.USER_KEYWORDS_SIMPLE + "/" + DateUtils.date2str(TEMP_DATE), FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<DocumentModel>() {
				@Override
				public String format(DocumentModel value) {
					return value.toString();
				}
			});

			today = DateUtils.getNextDay(today);
			Controller.run();
		} while (today.before(toDate));
	}

	void recommend() {
		String newsKeywordsPath = Controller.NEWS.KEYWORDS;
		String clickPath = Controller.MATCH.RESULT_4COLS;
		DataSet<Tuple2<Date, LinkedList<Long>>> uniqueUsers = generateUniqueUsers(clickPath, null);

		final DataSet<DocumentModel> newsKeywords = DocumentModel.loadDocumentDataSet(newsKeywordsPath);

		final CosineSimilarity similarity = new CosineSimilarity();
		final EuclideanDistance euclideanDistance = new EuclideanDistance();

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

			DataSet<DocumentModel> userKeywords = DocumentModel.loadDocumentDataSet(Controller.TWEETS.USER_KEYWORDS_SIMPLE).cross(todayUsers)
					.flatMap(new FlatMapFunction<Tuple2<DocumentModel, Tuple2<Date, LinkedList<Long>>>, DocumentModel>() {
						@Override
						public void flatMap(Tuple2<DocumentModel, Tuple2<Date, LinkedList<Long>>> input, Collector<DocumentModel> collector) throws Exception {
							if (input.f1.f1.contains(input.f0.userId)) {
								collector.collect(input.f0);
							}
						}
					});

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
						value = euclideanDistance.compute(input.f0.keywords, input.f1.keywords);
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
					for (Tuple3<Long, Long, Double> input : iterable) {
						if (first) {
							first = false;
							recommendation.userId = input.f0;
							recommendation.date = TEMP_DATE;
							recommendation.news = new LinkedList<Long>();
						}
						recommendation.news.add(input.f1);
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

		DateIterator dateIterator = new DateIterator(scene.fromDate, scene.toDate);
		for (final Date today : dateIterator) {
			DataSet<StatItem> statItemDataSet = precisionStats.precisions(scene.getRecommendationsPath(today), Controller.MATCH.RESULT_4COLS, scene.getPrecisionsPath(today), today, false);
			precisionStats.statAll(statItemDataSet, today, scene.getStatsPath(today));

			Controller.run();
		}

		dateIterator.reset();
		precisionStats.uniteAll(scene.getStatsFolder(), dateIterator);
	}

}
