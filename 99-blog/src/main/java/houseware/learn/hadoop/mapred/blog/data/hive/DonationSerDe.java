package houseware.learn.hadoop.mapred.blog.data.hive;

import houseware.learn.hadoop.mapred.blog.data.writable.DonationWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import java.util.*;

public class DonationSerDe extends AbstractSerDe {

    public static final Log LOG = LogFactory.getLog(DonationSerDe.class.getName());

    private static final int NUM_COLUMNS = 17;
    private List<String> columnNames;
    private StandardStructObjectInspector objInspector;
    private ArrayList<Object> row;

    @Override
    public Object deserialize(Writable obj) throws SerDeException {

        if (!(obj instanceof DonationWritable)) {
            throw new SerDeException(String.format("Expected type %s but received %s",
                    DonationWritable.class.getName(), obj.getClass().getName()));
        }

        DonationWritable donation = (DonationWritable) obj;
        row.set(0, donation.donation_id);
        row.set(1, donation.project_id);
        row.set(2, donation.donor_city);
        row.set(3, donation.donor_state);
        row.set(4, donation.donor_is_teacher);
        row.set(5, donation.ddate);
        row.set(6, donation.amount);
        row.set(7, donation.support);
        row.set(8, donation.total);
        row.set(9, donation.payment_method);
        row.set(10, donation.payment_inc_acct_credit);
        row.set(11, donation.payment_inc_campaign_gift_card);
        row.set(12, donation.payment_inc_web_gift_card);
        row.set(13, donation.payment_promo_matched);
        row.set(14, donation.via_giving_page);
        row.set(15, donation.thank_you_packet);
        row.set(16, donation.message);
        return row;
    }

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {

        LOG.info("Initialized DonationSerDe");

        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        columnNames = Arrays.asList(columnNameProperty.split(","));
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

        if (columnNames.size() != columnTypes.size()) {
            throw new SerDeException("Column names and Column types are not of same size.");
        }
        if (columnNames.size() != NUM_COLUMNS) {
            throw new SerDeException("Wrong number of columns received.");
        }

        // Create inspectors for each attribute
        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(columnNames.size());
        for (int i = 0; i <= 5; i++)
            inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        for (int i = 6; i <= 8; i++)
            inspectors.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
        for (int i = 9; i <= 16; i++)
            inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // StandardStruct uses ArrayList to store the row.
        objInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);

        // Constructing the row object which will be reused for all rows, setting all attributes to null.
        row = new ArrayList<Object>(Collections.nCopies(columnNames.size(), null));
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        return NullWritable.get();
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objInspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return NullWritable.class;
    }
}
