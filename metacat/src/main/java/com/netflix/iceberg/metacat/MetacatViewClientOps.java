package com.netflix.iceberg.metacat;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.netflix.bdp.view.BaseMetastoreViewOperations;
import com.netflix.bdp.view.CommonViewConstants;
import com.netflix.bdp.view.ViewVersionMetadata;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.dto.ViewDto;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatPreconditionFailedException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;

class MetacatViewClientOps extends BaseMetastoreViewOperations {
    private static final Logger LOG = LoggerFactory.getLogger(MetacatViewClientOps.class);
    private static final Predicate<Exception> RETRY_IF = exc ->
            !exc.getClass().getCanonicalName().contains("Unrecoverable");

    private Configuration conf;
    private final Client client;
    private final String catalog;
    private final String dbName;
    private final String viewName;
    private HadoopFileIO fileIO;

    private Map<String, String> extraProperties;

    MetacatViewClientOps(Configuration conf, Client client, String catalog, String dbName, String viewName) {
        this.conf = conf;
        this.client = client;
        this.catalog = catalog;
        this.dbName = dbName;
        this.viewName = viewName;
        refresh();
    }

    @Override
    public Map<String, String> extraProperties() {
        return extraProperties;
    }

    @Override
    public synchronized ViewVersionMetadata refresh() {
        String metadataLocation = null;
        try {
            TableDto tableInfo = client.getApi().getTable(catalog, dbName, viewName,
                    true /* send table fields, partition keys */,
                    true /* get user definition metadata */,
                    false /* do not send user data metadata (?) */);

            Map<String, String> tableProperties = tableInfo.getMetadata();

            String common_view_flag = tableProperties.get(CommonViewConstants.COMMON_VIEW);
            if (common_view_flag == null || !common_view_flag.equalsIgnoreCase("true")) {
                throw new NotFoundException(
                    "Invalid view, missing or wrong common view flag: %s.%s.%s", catalog, dbName, viewName);
            }

            metadataLocation = tableProperties.get(METADATA_LOCATION_PROP);
            if (metadataLocation == null) {
                throw new NotFoundException(
                    "Invalid view, missing metadata_location: %s.%s.%s", catalog, dbName, viewName);
            }

            extraProperties = extraPropertiesFromTable(tableInfo);
        } catch (MetacatNotFoundException e) {
            // if metadata has been loaded for this table and is now gone, throw an exception
            // otherwise, assume the table doesn't exist yet.
            if (currentMetadataLocation() != null) {
                throw new NotFoundException("No such Metacat view: %s.%s.%s", catalog, dbName, viewName);
            }
        }

        refreshFromMetadataLocation(metadataLocation, RETRY_IF, 20);

        return current();
    }

    private static Map<String, String> extraPropertiesFromTable(TableDto tableInfo) {
        final String owner = tableInfo.getDefinitionMetadata().get("owner").get("userId").asText();
        return ImmutableBiMap.of(CommonViewConstants.COMMON_VIEW, "true", "owner", owner);
    }

    @Override
    public synchronized void commit(ViewVersionMetadata base, ViewVersionMetadata metadata, Map<String, String> properties) {
        // if the metadata is already out of date, reject it
        if (base != current()) {
            throw new CommitFailedException("Cannot commit changes based on stale view metadata");
        }

        // if the metadata is not changed, return early
        if (base == metadata) {
            LOG.info("Nothing to commit.");
            return;
        }

        String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

        boolean threw = true;
        try {
            StorageDto serde = new StorageDto();
            serde.setUri(metadata.location());

            TableDto newTableInfo = new TableDto();
            newTableInfo.setName(QualifiedName.ofTable(catalog, dbName, viewName));
            newTableInfo.setSerde(serde);
            newTableInfo.setDataExternal(true);
            Map<String, String> metadata_props = new HashMap<>(properties);
            metadata_props.put(CommonViewConstants.COMMON_VIEW, "true");
            metadata_props.put(METADATA_LOCATION_PROP, newMetadataLocation);
            metadata_props.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation() == null ? "" : currentMetadataLocation());
            newTableInfo.setMetadata(metadata_props);
            ViewDto viewDto = new ViewDto();
            viewDto.setViewOriginalText("/** Common View **/");
            viewDto.setViewExpandedText("/** Common View **/");
            newTableInfo.setView(viewDto);

            if (base == null) {
                client.getApi().createTable(catalog, dbName, viewName, newTableInfo);
            } else {
                client.getApi().updateTable(catalog, dbName, viewName, newTableInfo);
            }
            threw = false;

        } catch (MetacatPreconditionFailedException e) {
            throw new CommitFailedException(e, "Failed to commit due to conflict");

        } catch (MetacatBadRequestException | MetacatUserMetadataException e) {
            throw new ValidationException(e,
                    "Failed to commit: invalid request", e.getMessage());

        } catch (MetacatException e) {
            throw new RuntimeIOException(new IOException(e), "Failed to commit");

        } finally {
            if (threw) {
                // if anything went wrong, clean up the uncommitted metadata file
                io().deleteFile(newMetadataLocation);
            }
        }

        requestRefresh();
    }

    @Override
    public synchronized void drop(String viewName) {
        // Callers must guarantee this is the table to delete
        // because we do not want to load table to verify table type.
        client.getApi().deleteTable(catalog, dbName, this.viewName);
    }

    @Override
    public FileIO io() {
        if (fileIO == null) {
            fileIO = new HadoopFileIO(conf);
        }

        return fileIO;
    }

    public static List<FieldDto> fieldDtos(Schema schema) {
        List<FieldDto> fields = Lists.newArrayList();

        for (int i = 0; i < schema.columns().size(); i++) {
            final Types.NestedField field = schema.columns().get(i);
            FieldDto fieldInfo = new FieldDto();
            fieldInfo.setPos(i);
            fieldInfo.setName(field.name());
            fieldInfo.setType(getMetacatTypeName(field.type()));
            fieldInfo.setIsNullable(field.isOptional());

            fields.add(fieldInfo);
        }
        return fields;
    }

    public static String getMetacatTypeName(Type type) {
        switch (type.typeId()) {
            case BOOLEAN:
                return "boolean";
            case INTEGER:
                return "int";
            case LONG:
                return "bigint";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DATE:
                return "date";
            case TIME:
                throw new UnsupportedOperationException("Metacat does not support time fields");
            case TIMESTAMP:
                return "timestamp";
            case STRING:
            case UUID:
                return "string";
            case FIXED:
                return "binary";
            case BINARY:
                return "binary";
            case DECIMAL:
                final Types.DecimalType decimalType = (Types.DecimalType) type;
                return format("decimal(%s,%s)", decimalType.precision(), decimalType.scale()); //TODO may be just decimal?
            case STRUCT:
                final Types.StructType structType = type.asStructType();
                final String nameToType = structType.fields().stream().map(
                        f -> format("%s:%s", f.name(), getMetacatTypeName(f.type()))
                ).collect(Collectors.joining(","));
                return format("struct<%s>", nameToType);
            case LIST:
                final Types.ListType listType = type.asListType();
                return format("array<%s>", getMetacatTypeName(listType.elementType()));
            case MAP:
                final Types.MapType mapType = type.asMapType();
                return format("map<%s,%s>", getMetacatTypeName(mapType.keyType()), getMetacatTypeName(mapType.valueType()));
            default:
                throw new UnsupportedOperationException(type + " is not supported");
        }
    }
}
