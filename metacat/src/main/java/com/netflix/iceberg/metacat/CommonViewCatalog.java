package com.netflix.iceberg.metacat;

import com.google.common.base.Suppliers;
import com.netflix.bdp.view.UpdateProperties;
import com.netflix.bdp.view.ViewDefinition;
import com.netflix.bdp.view.Views;
import java.util.Arrays;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.ViewChange;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class CommonViewCatalog implements ViewCatalog {

  private String name;
  private final Supplier<Views> viewsSupplier = Suppliers.memoize(this::buildViews);
  private final Supplier<SparkSession> sparkSessionSupplier = Suppliers.memoize(SparkSession::active);

  @Override
  public View loadView(Identifier ident) throws NoSuchViewException {
    com.netflix.bdp.view.View view = loadViewInternal(ident);
    return CommonView.of(
        buildViewIdentifier(ident),
        view.currentVersion().viewDefinition(),
        view.properties());
  }

  @Override
  public void createView(
      Identifier ident,
      String sql,
      StructType schema,
      String[] catalogAndNamespace,
      Map<String, String> properties)
      throws ViewAlreadyExistsException, NoSuchNamespaceException {
    String viewIdentifier = buildViewIdentifier(ident);
    ViewDefinition viewDefinition = ViewDefinition.of(
        sql,
        SparkSchemaUtil.convert(schema),
        catalogAndNamespace[0],
        Arrays.asList(catalogAndNamespace).subList(1, catalogAndNamespace.length));
    try {
      lazyViews().create(viewIdentifier, viewDefinition, properties);
    } catch (AlreadyExistsException e) {
      throw new ViewAlreadyExistsException(ident, e);
    }
  }

  @Override
  public void replaceView(
      Identifier ident,
      String sql,
      StructType schema,
      String[] catalogAndNamespace,
      Map<String, String> properties)
      throws NoSuchViewException, NoSuchNamespaceException {
    String viewIdentifier = buildViewIdentifier(ident);
    ViewDefinition viewDefinition = ViewDefinition.of(
        sql,
        SparkSchemaUtil.convert(schema),
        catalogAndNamespace[0],
        Arrays.asList(catalogAndNamespace).subList(1, catalogAndNamespace.length));
    try {
      lazyViews().replace(viewIdentifier, viewDefinition, properties);
    } catch (NotFoundException e) {
      throw new NoSuchViewException(ident, e);
    }
  }

  @Override
  public void alterView(Identifier ident, ViewChange... changes) throws NoSuchViewException {
    com.netflix.bdp.view.View view = loadViewInternal(ident);
    UpdateProperties updateProperties = view.updateProperties();
    for (ViewChange change : changes) {
      applyChange(updateProperties, change);
    }
    updateProperties.commit();
  }

  @Override
  public boolean dropView(Identifier ident) {
    try {
      lazyViews().drop(buildViewIdentifier(ident));
      return true;
    } catch (RuntimeIOException e) {
      return false;
    }
  }

  @Override
  public void renameView(Identifier oldIdent, Identifier newIdent)
      throws NoSuchViewException, ViewAlreadyExistsException {
    if (!viewExists(oldIdent)) {
      throw new NoSuchViewException(oldIdent);
    }

    if (viewExists(newIdent)) {
      throw new ViewAlreadyExistsException(newIdent);
    }

    lazyViews().rename(buildViewIdentifier(oldIdent), buildViewIdentifier(newIdent));
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  private Views lazyViews() {
    return viewsSupplier.get();
  }

  private Views buildViews() {
    return new MetacatViewCatalog(sparkSessionSupplier.get().sessionState().newHadoopConf(), "spark");
  }

  private String buildViewIdentifier(Identifier ident) {
    StringJoiner joiner = new StringJoiner(".");
    joiner.add(name());
    for (String part : ident.namespace()) {
      joiner.add(part);
    }
    joiner.add(ident.name());
    return joiner.toString();
  }

  private com.netflix.bdp.view.View loadViewInternal(Identifier ident)
      throws NoSuchViewException {
    String viewIdentifier = buildViewIdentifier(ident);
    try {
      return lazyViews().load(viewIdentifier);
    } catch (NotFoundException e) {
      throw new NoSuchViewException(ident, e);
    }
  }

  private static void applyChange(UpdateProperties pendingUpdate, ViewChange change) {
    if (change instanceof ViewChange.SetProperty) {
      ViewChange.SetProperty set = (ViewChange.SetProperty) change;
      pendingUpdate.set(set.property(), set.value());
    } else if (change instanceof ViewChange.RemoveProperty) {
      ViewChange.RemoveProperty remove = (ViewChange.RemoveProperty) change;
      pendingUpdate.remove(remove.property());
    }
  }
}
