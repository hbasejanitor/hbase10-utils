package org.hbasejanitor.flume.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeRoutingRules {
  private List<Rule> routeRules = new ArrayList<>();
  private List<Rule> dropRules = new ArrayList<>();
  private static final Logger LOG = LoggerFactory.getLogger(FlumeRoutingRules.class);
  
  public void addRouteRule(Rule rule) {
    this.routeRules.add(rule);
  }
  
  public void addDropRule(Rule rule) {
    this.dropRules.add(rule);
  }
  
  
  public boolean route(final TableName table, final byte []columnFamily,
      final byte[] qualifer) {
    
    
    boolean drop= !CollectionUtils.isEmpty(dropRules.stream()
      .map((rule)->rule.match(table, columnFamily, qualifer))
      .filter((match)->match)
      .collect(Collectors.toList()));

    if (drop) {
      return false;
    }
    
    return !CollectionUtils.isEmpty(routeRules.stream()
      .map((rule)->rule.match(table, columnFamily, qualifer))
      .filter((match)->match)
      .collect(Collectors.toList()));
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    if (!CollectionUtils.isEmpty(dropRules)) {
      builder.append("Drop rules \n");
      dropRules.stream().forEach((rule)-> {builder.append(rule).append("\n");} );
    }
    if (!CollectionUtils.isEmpty(this.routeRules)) {
      builder.append("Allow rules \n");
      routeRules.stream().forEach((rule)-> {builder.append(rule).append("\n");} );
    }
    return builder.toString();
  }
  

}
