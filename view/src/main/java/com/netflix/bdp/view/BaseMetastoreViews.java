package com.netflix.bdp.view;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;

public abstract class BaseMetastoreViews implements Views {
    private final Configuration conf;

    public BaseMetastoreViews(Configuration conf) {
        this.conf = conf;
    }

    protected abstract BaseMetastoreViewOperations newViewOps(TableIdentifier viewName);

    protected String defaultWarehouseLocation(TableIdentifier viewIdentifier) {
        throw new UnsupportedOperationException("Implementation for 'defaultWarehouseLocation' not provided.");
    }

    @Override
    public void create(String viewIdentifier, ViewDefinition definition, Map<String, String> properties) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        if (ops.current() != null) {
            throw new AlreadyExistsException("View already exists: " + viewName);
        }

        String location = defaultWarehouseLocation(viewName);
        int parentId = -1;

        ViewUtils.doCommit(DDLOperations.CREATE, properties, 1, parentId, definition, location, ops, null);

    }

    @Override
    public void replace(String viewIdentifier, ViewDefinition definition, Map<String, String> properties) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        if (ops.current() == null) {
            throw new AlreadyExistsException("View " + viewName.toString() + " is expected to exist");
        }

        ViewVersionMetadata prevViewVersionMetadata = ops.current();
        Preconditions.checkState(prevViewVersionMetadata.versions().size() > 0, "Version history not found");
        int parentId = prevViewVersionMetadata.currentVersionId();

        String location = prevViewVersionMetadata.location();

        ViewUtils.doCommit(DDLOperations.REPLACE, properties, parentId + 1, parentId, definition, location, ops, prevViewVersionMetadata);
    }

    @Override
    public void drop(String viewIdentifier) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        ops.drop(viewName.toString());
    }

    @Override
    public View load(String viewIdentifier) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        if (ops.current() == null) {
            throw new NotFoundException("View does not exist: " + viewName);
        }
        return new BaseView(ops, viewName.toString());
    }

    @Override
    public ViewDefinition loadDefinition(String viewIdentifier) {
        TableIdentifier viewName = toCatalogTableIdentifier(viewIdentifier);
        ViewOperations ops = newViewOps(viewName);
        if (ops.current() == null) {
            throw new NotFoundException("View does not exist: " + viewName);
        }
        return ops.current().definition();
    }

    protected TableIdentifier toCatalogTableIdentifier(String tableIdentifier) {
        List<String> namespace = Lists.newArrayList();
        Iterable<String> parts = Splitter.on(".").split(tableIdentifier);

        String lastPart = "";
        for (String part : parts) {
            if (!lastPart.isEmpty()) {
                namespace.add(lastPart);
            }
            lastPart = part;
        }

        Preconditions.checkState(namespace.size() == 2, "Catalog and schema are expected in the namespace.");

        return TableIdentifier.of(Namespace.of(namespace.toArray(new String[namespace.size()])), lastPart);
    }
}
