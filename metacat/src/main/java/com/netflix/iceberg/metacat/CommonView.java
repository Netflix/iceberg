package com.netflix.iceberg.metacat;

import com.netflix.bdp.view.ViewDefinition;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.types.StructType;

public final class CommonView implements View {

  private final String name;
  private final ViewDefinition viewDefinition;
  private final Map<String, String> properties;

  CommonView(String name, ViewDefinition viewDefinition, Map<String, String> properties) {
    this.name = name;
    this.viewDefinition = viewDefinition;
    this.properties = properties;
  }

  public static CommonView of(String name, ViewDefinition viewDefinition, Map<String, String> properties) {
    return new CommonView(name, viewDefinition, properties);
  }

  @Override
  public String sql() {
    return viewDefinition.sql();
  }

  @Override
  public StructType schema() {
    return SparkSchemaUtil.convert(viewDefinition.schema());
  }

  @Override
  public String currentCatalog() {
    return viewDefinition.sessionCatalog();
  }

  @Override
  public String[] currentNamespace() {
    return viewDefinition.sessionNamespace().toArray(new String[0]);
  }

  @Override
  public Map<String, String> properties() {return properties;}

  @Override
  public String toString() {
    return "CommonView{" +
        "name='" + name + '\'' +
        ", viewDefinition=" + viewDefinition +
        ", properties=" + properties +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CommonView that = (CommonView) o;
    return Objects.equals(name, that.name) &&
        Objects.equals(viewDefinition, that.viewDefinition) &&
        Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, viewDefinition, properties);
  }
}
