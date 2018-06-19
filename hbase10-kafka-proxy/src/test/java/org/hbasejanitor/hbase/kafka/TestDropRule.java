/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.hbasejanitor.hbase.kafka;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test different cases of drop rules.
 */
public class TestDropRule {
  private static final String DROP_RULE1 =
      "<rules><rule action=\"drop\" table=\"default:MyTable\" /></rules>";
  private static final String DROP_RULE2 =
      "<rules><rule action=\"drop\" table=\"default:MyTable\" "
      + "columnFamily=\"data\"/></rules>";
  private static final String DROP_RULE3 =
      "<rules><rule action=\"drop\" table=\"default:MyTable\" "
      + "columnFamily=\"data\" qualifier=\"dhold\"/></rules>";

  private static final String DROP_RULE4 =
      "<rules><rule action=\"drop\" table=\"default:MyTable\" "
      + "columnFamily=\"data\" qualifier=\"dhold:*\"/></rules>";
  private static final String DROP_RULE5 =
      "<rules><rule action=\"drop\" table=\"default:MyTable\" "
      + "columnFamily=\"data\" qualifier=\"*pickme\"/></rules>";

  private static final String DROP_RULE6 =
      "<rules><rule action=\"drop\" table=\"default:MyTable\" "
      + "columnFamily=\"data\" qualifier=\"*pickme*\"/></rules>";

  @Test
  public void testDropies1() {
    TopicRoutingRules rules = new TopicRoutingRules();
    try {
      rules.parseRules(new ByteArrayInputStream(DROP_RULE1.getBytes()));
      Assert.assertEquals(1, rules.getDropRules().size());
      Assert.assertEquals(TableName.valueOf("default:MyTable"),
        rules.getDropRules().get(0).getTableName());
      Assert.assertEquals(null, rules.getDropRules().get(0).getColumnFamily());
      Assert.assertEquals(null, rules.getDropRules().get(0).getQualifier());
      Assert.assertEquals(0, rules.getRouteRules().size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDropies2() {
    TopicRoutingRules rules = new TopicRoutingRules();
    try {
      rules.parseRules(new ByteArrayInputStream(DROP_RULE2.getBytes()));
      Assert.assertEquals(1, rules.getDropRules().size());
      Assert.assertEquals(TableName.valueOf("default:MyTable"),
        rules.getDropRules().get(0).getTableName());
      Assert.assertTrue(
        Bytes.equals("data".getBytes(), rules.getDropRules().get(0).getColumnFamily()));
      Assert.assertEquals(null, rules.getDropRules().get(0).getQualifier());
      Assert.assertEquals(0, rules.getRouteRules().size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDropies3() {
    TopicRoutingRules rules = new TopicRoutingRules();
    try {
      rules.parseRules(new ByteArrayInputStream(DROP_RULE3.getBytes()));
      Assert.assertEquals(1, rules.getDropRules().size());
      Assert.assertEquals(TableName.valueOf("default:MyTable"),
        rules.getDropRules().get(0).getTableName());
      Assert.assertTrue(
        Bytes.equals("data".getBytes(), rules.getDropRules().get(0).getColumnFamily()));
      Assert
          .assertTrue(Bytes.equals("dhold".getBytes(), rules.getDropRules().get(0).getQualifier()));
      Assert.assertEquals(0, rules.getRouteRules().size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDropies4() {
    TopicRoutingRules rules = new TopicRoutingRules();
    try {
      rules.parseRules(new ByteArrayInputStream(DROP_RULE4.getBytes()));
      Assert.assertEquals(1, rules.getDropRules().size());
      Assert.assertEquals(TableName.valueOf("default:MyTable"),
        rules.getDropRules().get(0).getTableName());
      Assert.assertTrue(
        Bytes.equals("data".getBytes(), rules.getDropRules().get(0).getColumnFamily()));
      Assert.assertTrue(
        Bytes.equals("dhold:".getBytes(), rules.getDropRules().get(0).getQualifier()));
      Assert.assertEquals(0, rules.getRouteRules().size());

      DropRule drop = rules.getDropRules().get(0);
      Assert.assertFalse(
        drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(), "blah".getBytes()));
      Assert.assertFalse(
        drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(), "dholdme".getBytes()));
      Assert.assertTrue(
        drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(), "dhold:me".getBytes()));

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDropies5() {
    TopicRoutingRules rules = new TopicRoutingRules();
    try {
      rules.parseRules(new ByteArrayInputStream(DROP_RULE5.getBytes()));
      Assert.assertEquals(1, rules.getDropRules().size());
      Assert.assertEquals(TableName.valueOf("default:MyTable"),
        rules.getDropRules().get(0).getTableName());
      Assert.assertTrue(
        Bytes.equals("data".getBytes(), rules.getDropRules().get(0).getColumnFamily()));
      Assert.assertTrue(
        Bytes.equals("pickme".getBytes(), rules.getDropRules().get(0).getQualifier()));
      Assert.assertEquals(0, rules.getRouteRules().size());

      DropRule drop = rules.getDropRules().get(0);
      Assert.assertFalse(
        drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(), "blah".getBytes()));
      Assert.assertFalse(drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(),
        "blacickme".getBytes()));
      Assert.assertTrue(drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(),
        "hithere.pickme".getBytes()));

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDropies6() {
    TopicRoutingRules rules = new TopicRoutingRules();
    try {
      rules.parseRules(new ByteArrayInputStream(DROP_RULE6.getBytes()));
      Assert.assertEquals(1, rules.getDropRules().size());
      Assert.assertEquals(TableName.valueOf("default:MyTable"),
        rules.getDropRules().get(0).getTableName());
      Assert.assertTrue(
        Bytes.equals("data".getBytes(), rules.getDropRules().get(0).getColumnFamily()));
      Assert.assertTrue(
        Bytes.equals("pickme".getBytes(), rules.getDropRules().get(0).getQualifier()));
      Assert.assertEquals(0, rules.getRouteRules().size());

      DropRule drop = rules.getDropRules().get(0);
      Assert.assertFalse(
        drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(), "blah".getBytes()));
      Assert.assertFalse(drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(),
        "blacickme".getBytes()));
      Assert.assertTrue(drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(),
        "hithere.pickme".getBytes()));
      Assert.assertTrue(drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(),
        "pickme.pleaze.do.it".getBytes()));
      Assert.assertFalse(drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(),
        "please.pickme.pleaze".getBytes()));
      Assert.assertTrue(drop.match(TableName.valueOf("default:MyTable"), "data".getBytes(),
        "pickme.pleaze.pickme".getBytes()));

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

}
