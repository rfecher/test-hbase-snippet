package test;

import java.io.IOException;
import java.util.Date;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.test.HBaseStoreTestEnvironment;
import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;

public class TestHBaseSnippet
{
	public static void main(
			final String[] args ) throws Exception {
		 ZookeeperTestEnvironment.getInstance().setup();
		HBaseStoreTestEnvironment.getInstance().setup();
		
		HBaseStoreTestEnvironment.getInstance().tearDown();
		ZookeeperTestEnvironment.getInstance().tearDown();
	}
	public static void ingestSomeData() throws Exception{
		for (double x = -5; x <= 5; x++) {
			for (double y = -5; y <= 5; y++) {
				final String rowId = x + "_" + y;
				indexFeatures(
						"entity_" + rowId,
						new ByteArrayId(
								rowId).getBytes(),
						new GeometryFactory().createPoint(
								new Coordinate(
										x,
										y)));
			}
		}
	}

	private static SimpleFeatureType createMyFeatureType() {

		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder ab = new AttributeTypeBuilder();

		builder.setName(
				"MyFeatures");
		builder.add(
				ab
						.binding(
								Geometry.class)
						.nillable(
								false)
						.buildDescriptor(
								"geometry"));
		builder.add(
				ab
						.binding(
								Date.class)
						.nillable(
								true)
						.buildDescriptor(
								"timeStamp"));
		builder.add(
				ab
						.binding(
								String.class)
						.nillable(
								true)
						.buildDescriptor(
								"entity"));

		return builder.buildFeatureType();
	}

	public static void indexFeatures(
			final String entityName,
			final byte[] rowId,
			final Geometry value )
					throws Exception {
		final SimpleFeatureType mySimpleFeatureType = createMyFeatureType();
		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				mySimpleFeatureType);

		final SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				mySimpleFeatureType);
		sfBuilder.set(
				"geometry",
				value);
		sfBuilder.set(
				"entity",
				entityName);
		sfBuilder.set(
				"timeStamp",
				new Date());

		final SimpleFeature feature = sfBuilder.buildFeature(
				new String(
						rowId));

		final PrimaryIndex index = new SpatialIndexBuilder().createIndex();

		final BasicHBaseOperations instance = new BasicHBaseOperations(
				ZookeeperTestEnvironment.getInstance().getZookeeper(),
				"geowave.rasters");

		final HBaseOptions hbaseOptions = new HBaseOptions();

		final HBaseAdapterStore adapterStore = new HBaseAdapterStore(
				instance);

		final HBaseDataStore geowaveDataStore = new HBaseDataStore(
				new HBaseIndexStore(
						instance),
				adapterStore,
				new HBaseDataStatisticsStore(
						instance),
				new HBaseAdapterIndexMappingStore(
						instance),
				new HBaseSecondaryIndexDataStore(
						instance),
				instance,
				hbaseOptions);

		hbaseOptions.setCreateTable(
				true);
		hbaseOptions.setUseAltIndex(
				true);
		hbaseOptions.setPersistDataStatistics(
				true);

		try (IndexWriter indexWriter = geowaveDataStore.createWriter(
				adapter,
				index)) {
			indexWriter.write(
					feature);
		}
		catch (final IOException e) {
			throw new Exception(
					e);
		}
	}
}
