package charley.wu.flink.datahub.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static charley.wu.flink.datahub.config.DataHubConfig.*;


@Internal
public class DataHubSourceFactory implements DynamicTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();

        requiredOptions.add(SOURCE_PROJECT);
        requiredOptions.add(SOURCE_TOPIC);
        requiredOptions.add(SOURCE_SUBID);

        requiredOptions.add(ACCESS_ID);
        requiredOptions.add(ACCESS_KEY);
        requiredOptions.add(ENDPOINT);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper
                factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);

//        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = factoryHelper
//            .discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);

        factoryHelper.validate();

        final DataType rowType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        final List<String> columnNames = context.getCatalogTable().getResolvedSchema().getColumns().stream()
                .filter(Column::isPhysical)
                .map(Column::getName)
                .collect(Collectors.toList());


        return new DatahubTableSource(factoryHelper.getOptions(), columnNames);
    }

}
