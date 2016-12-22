package test;

import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.test.HBaseStoreTestEnvironment;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;

public class HBaseSnippetTestCase
{
	@Test
	public void testIngestAndQuery()
			throws Exception {
		try {
			ZookeeperTestEnvironment.getInstance().setup();
			HBaseStoreTestEnvironment.getInstance().setup();
			TestHBaseSnippet.ingestSomeData();
			final BasicHBaseOperations operations = new BasicHBaseOperations(
					ZookeeperTestEnvironment.getInstance().getZookeeper(),
					"geowave.rasters");

			final HBaseDataStore geowaveDataStore = new HBaseDataStore(
					operations);

			// it should return 9 results between -1 and 1 in both x and y
			int resultCnt = 0;
			try (CloseableIterator<SimpleFeature> results = geowaveDataStore.query(
					new QueryOptions(),
					new SpatialQuery(
							new GeometryFactory().toGeometry(
									new Envelope(
											-1,
											1,
											-1,
											1))))) {
				while (results.hasNext()) {
					SimpleFeature f = results.next();
					double x = ((Geometry) f.getDefaultGeometry()).getCentroid().getX();
					double y = ((Geometry) f.getDefaultGeometry()).getCentroid().getY();
					assert (x >= -1 && x <= 1 && y >= -1 && y <= 1);
					resultCnt++;
				}
			}
			assert (resultCnt == 9);
		}
		finally {
			HBaseStoreTestEnvironment.getInstance().tearDown();
			ZookeeperTestEnvironment.getInstance().tearDown();
		}
	}
}
