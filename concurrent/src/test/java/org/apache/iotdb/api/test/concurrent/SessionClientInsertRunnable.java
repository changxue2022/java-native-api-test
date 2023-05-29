package org.apache.iotdb.api.test.concurrent;

import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.List;

import static java.lang.System.out;

public class SessionClientInsertRunnable implements Runnable {
    private SessionPool session;
    private boolean isAligned;
    private String device;
    private Tablet tablet;
    private List<MeasurementSchema> schemaList;
    private int batchSize = 100;
    public SessionClientInsertRunnable(SessionPool session, String device, int insertCount,
                                       boolean isAligned, List<MeasurementSchema> schemaList) {
        this.session = session;
        this.device = device;
        this.isAligned = isAligned;
        this.batchSize = insertCount;
        this.schemaList = schemaList;
        tablet = new Tablet(this.device, schemaList, batchSize);
        int rowIndex = 0;
        for (int row = 0; row < batchSize; row++) {
            rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, System.currentTimeMillis());
            for (int i = 0; i < schemaList.size(); i++) {
                switch (schemaList.get(i).getType()) {
                    case BOOLEAN:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, true);
                        break;
                    case INT32:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, row);
                        break;
                    case INT64:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, System.currentTimeMillis());
                        break;
                    case FLOAT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, (float)System.nanoTime());
                        break;
                    case DOUBLE:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, System.nanoTime()+0.3245);
                        break;
                    case TEXT:
                        tablet.addValue(schemaList.get(i).getMeasurementId(), rowIndex, device);
                        break;
                }
            }
        }
//        out.println(this.device);
//        out.println(schemaList.size());
//        out.println(tablet.values);
    }
    @Override
    public void run()  {
        try {
            long startTime = System.currentTimeMillis();
            session.insertTablet(tablet);
            long elapseTime = System.currentTimeMillis() - startTime;
            out.println(Thread.currentThread().getName() + " device=" + device + " cost:" + elapseTime);
            tablet.reset();
        } catch (Exception e) {
            out.println(Thread.currentThread().getName()+" device="+device +e);
        }
    }
}