package de.tuberlin.dima.lyc.algorithm;

import de.tuberlin.dima.lyc.Controller;
import de.tuberlin.dima.lyc.PrecisionStats;
import de.tuberlin.dima.lyc.models.ClickModel;
import de.tuberlin.dima.lyc.models.RecommendationModel;
import de.tuberlin.dima.lyc.models.Scene;
import de.tuberlin.dima.lyc.models.StatItem;
import de.tuberlin.dima.lyc.utils.DateFilter;
import de.tuberlin.dima.lyc.utils.DateIterator;
import de.tuberlin.dima.lyc.utils.DateZoneFilter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by zhy1378 on 15.06.15.
 */
public class HotTopics implements Serializable {
	private Scene scene;

	public HotTopics(Scene scene) {
		this.scene = scene;
	}

	public void recommend() {
		DataSet<ClickModel> clickModelDataSet = ClickModel.loadClickDataSet(Controller.MATCH.DAILY_STATS);

		DateIterator dateIterator = new DateIterator(scene.fromDate, scene.toDate);
		for (Date today : dateIterator) {
			// 过滤出对today 有效的点击。
			DataSet<ClickModel> dataSetToday = filterOutClicks(clickModelDataSet, scene.numDays, today);
			// 根据点击生成推荐。
			DataSet<RecommendationModel> recommendationDataSet = dataSetToday.groupBy("date").sortGroup("count", Order.DESCENDING).reduceGroup(new GroupReduceFunction<ClickModel, RecommendationModel>() {
				@Override
				public void reduce(Iterable<ClickModel> iterable, Collector<RecommendationModel> collector) throws Exception {
					collector.collect(RecommendationModel.fromClickList(iterable, scene.numRecommendations));
				}
			});

			recommendationDataSet.writeAsFormattedText(scene.getRecommendationsPath(today), FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<RecommendationModel>() {
				@Override
				public String format(RecommendationModel value) {
					return value.toString();
				}
			});

			Controller.run();
		}
	}


	/**
	 * 根据特定场景生成topic 列表。
	 * 在场景中，可以设置日期。
	 *
	 * @param clicksDataSet
	 * @return
	 */
	public DataSet<ClickModel> filterOutClicks(DataSet<ClickModel> clicksDataSet, int numDays, final Date today) {
		final DateFilter dateFilter = new DateZoneFilter(today, numDays, 0);
		DataSet<ClickModel> stats = clicksDataSet.filter(new FilterFunction<ClickModel>() {
			@Override
			public boolean filter(ClickModel clickModel) throws Exception {
				return dateFilter.filter(clickModel.date);
			}
		})
				.groupBy("date", "newsId").reduceGroup(new GroupReduceFunction<ClickModel, ClickModel>() {
					@Override
					public void reduce(Iterable<ClickModel> iterable, Collector<ClickModel> collector) throws Exception {
						int count = 0;
						ClickModel output = new ClickModel();

						for (ClickModel input : iterable) {
							if (count == 0) {
								output.newsId = input.newsId;
								output.date = today;
							}
							count += input.count;
						}
						output.count = count;
						collector.collect(output);
					}
				});
		return stats;
	}

	public void precisionStats() {
		PrecisionStats precisionStats = new PrecisionStats();

		DateIterator dateIterator = new DateIterator(scene.fromDate, scene.toDate);
		for (Date today : dateIterator) {
			DataSet<StatItem> statItemDataSet = precisionStats.precisions(scene.getRecommendationsPath(today), Controller.MATCH.RESULT_4COLS, scene.getPrecisionsPath(today), today, true);
			precisionStats.statAll(statItemDataSet, today, scene.getStatsPath(today));
			Controller.run();
		}
	}

	public static void main(String... args) {
		HotTopics hotTopics = new HotTopics(Controller.config.getCurrentScene());
		//hotTopics.recommend();
		hotTopics.precisionStats();
	}
}
