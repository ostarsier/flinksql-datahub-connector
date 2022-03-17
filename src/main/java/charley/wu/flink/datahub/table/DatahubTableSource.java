package charley.wu.flink.datahub.table;

import charley.wu.flink.datahub.DataHubSource;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

import java.util.List;

public class DatahubTableSource implements ScanTableSource {

    private ReadableConfig props;
    private List<String> columnNames;

    public DatahubTableSource(ReadableConfig props, List<String> columnNames) {
        this.props = props;
        this.columnNames = columnNames;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {
        boolean bounded = false;
        final DataHubSource source = new DataHubSource(props, columnNames);
        return SourceFunctionProvider.of(source, bounded);
    }

    @Override
    public DynamicTableSource copy() {
        return new DatahubTableSource(props, columnNames);
    }

    @Override
    public String asSummaryString() {
        return getClass().getSimpleName();
    }


}