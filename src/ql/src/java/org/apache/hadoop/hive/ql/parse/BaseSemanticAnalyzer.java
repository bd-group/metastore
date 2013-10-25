/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionConstants;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionDefinition;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * BaseSemanticAnalyzer.
 *
 */
public abstract class BaseSemanticAnalyzer {
  protected final Hive db;
  protected final HiveConf conf;
  protected List<Task<? extends Serializable>> rootTasks;
  protected FetchTask fetchTask;
  protected final Log LOG;
  public static Log zlog = LogFactory.getLog(BaseSemanticAnalyzer.class.getName());
  protected final LogHelper console;

  protected Context ctx;
  protected HashMap<String, String> idToTableNameMap;
  protected QueryProperties queryProperties;

  public static int HIVE_COLUMN_ORDER_ASC = 1;
  public static int HIVE_COLUMN_ORDER_DESC = 0;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  protected HashSet<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected HashSet<WriteEntity> outputs;
  /**
   * Lineage information for the query.
   */
  protected LineageInfo linfo;
  protected TableAccessInfo tableAccessInfo;

  protected static final String TEXTFILE_INPUT = TextInputFormat.class
      .getName();
  protected static final String TEXTFILE_OUTPUT = IgnoreKeyTextOutputFormat.class
      .getName();
  protected static final String SEQUENCEFILE_INPUT = SequenceFileInputFormat.class
      .getName();
  protected static final String SEQUENCEFILE_OUTPUT = SequenceFileOutputFormat.class
      .getName();
  protected static final String RCFILE_INPUT = RCFileInputFormat.class
      .getName();
  protected static final String RCFILE_OUTPUT = RCFileOutputFormat.class
      .getName();
  protected static final String COLUMNAR_SERDE = ColumnarSerDe.class.getName();

  class RowFormatParams {
    String fieldDelim = null;
    String fieldEscape = null;
    String collItemDelim = null;
    String mapKeyDelim = null;
    String lineDelim = null;

    protected void analyzeRowFormat(AnalyzeCreateCommonVars shared, ASTNode child) throws SemanticException {
      child = (ASTNode) child.getChild(0);
      int numChildRowFormat = child.getChildCount();
      for (int numC = 0; numC < numChildRowFormat; numC++) {
        ASTNode rowChild = (ASTNode) child.getChild(numC);
        switch (rowChild.getToken().getType()) {
        case HiveParser.TOK_TABLEROWFORMATFIELD:
          fieldDelim = unescapeSQLString(rowChild.getChild(0)
              .getText());
          if (rowChild.getChildCount() >= 2) {
            fieldEscape = unescapeSQLString(rowChild
                .getChild(1).getText());
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
          collItemDelim = unescapeSQLString(rowChild
              .getChild(0).getText());
          break;
        case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
          mapKeyDelim = unescapeSQLString(rowChild.getChild(0)
              .getText());
          break;
        case HiveParser.TOK_TABLEROWFORMATLINES:
          lineDelim = unescapeSQLString(rowChild.getChild(0)
              .getText());
          if (!lineDelim.equals("\n")
              && !lineDelim.equals("10")) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(rowChild,
                ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg()));
          }
          break;
        default:
          assert false;
        }
      }
    }
  }

  class AnalyzeCreateCommonVars {
    String serde = null;
    Map<String, String> serdeProps = new HashMap<String, String>();
  }

  class StorageFormat {
    String inputFormat = null;
    String outputFormat = null;
    String storageHandler = null;

    protected boolean fillStorageFormat(ASTNode child, AnalyzeCreateCommonVars shared) {
      boolean storageFormat = false;
      switch(child.getToken().getType()) {
      case HiveParser.TOK_TBLSEQUENCEFILE:
        inputFormat = SEQUENCEFILE_INPUT;
        outputFormat = SEQUENCEFILE_OUTPUT;
        storageFormat = true;
        break;
      case HiveParser.TOK_TBLTEXTFILE:
        inputFormat = TEXTFILE_INPUT;
        outputFormat = TEXTFILE_OUTPUT;
        storageFormat = true;
        break;
      case HiveParser.TOK_TBLRCFILE:
        inputFormat = RCFILE_INPUT;
        outputFormat = RCFILE_OUTPUT;
        if (shared.serde == null) {
          shared.serde = COLUMNAR_SERDE;
        }
        storageFormat = true;
        break;
      case HiveParser.TOK_TABLEFILEFORMAT:
        inputFormat = unescapeSQLString(child.getChild(0).getText());
        outputFormat = unescapeSQLString(child.getChild(1).getText());
        storageFormat = true;
        break;
      case HiveParser.TOK_STORAGEHANDLER:
        storageHandler = unescapeSQLString(child.getChild(0).getText());
        if (child.getChildCount() == 2) {
          readProps(
            (ASTNode) (child.getChild(1).getChild(0)),
            shared.serdeProps);
        }
        storageFormat = true;
        break;
      }
      return storageFormat;
    }

    protected void fillDefaultStorageFormat(AnalyzeCreateCommonVars shared) {
      if ((inputFormat == null) && (storageHandler == null)) {
        if ("SequenceFile".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
          inputFormat = SEQUENCEFILE_INPUT;
          outputFormat = SEQUENCEFILE_OUTPUT;
        } else if ("RCFile".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
          inputFormat = RCFILE_INPUT;
          outputFormat = RCFILE_OUTPUT;
          shared.serde = COLUMNAR_SERDE;
        } else {
          inputFormat = TEXTFILE_INPUT;
          outputFormat = TEXTFILE_OUTPUT;
        }
      }
    }

  }

  public BaseSemanticAnalyzer(HiveConf conf) throws SemanticException {
    try {
      this.conf = conf;
      db = Hive.get(conf);
      rootTasks = new ArrayList<Task<? extends Serializable>>();
      LOG = LogFactory.getLog(this.getClass().getName());
      console = new LogHelper(LOG);
      idToTableNameMap = new HashMap<String, String>();
      inputs = new LinkedHashSet<ReadEntity>();
      outputs = new LinkedHashSet<WriteEntity>();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public HashMap<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public abstract void analyzeInternal(ASTNode ast) throws SemanticException;
  public void init() {
    //no-op
  }

  public void initCtx(Context ctx) {
    this.ctx = ctx;
  }

  public void analyze(ASTNode ast, Context ctx) throws SemanticException {
    initCtx(ctx);
    init();
    analyzeInternal(ast);
  }

  public void validate() throws SemanticException {
    // Implementations may choose to override this
  }

  public List<Task<? extends Serializable>> getRootTasks() {
    return rootTasks;
  }

  /**
   * @return the fetchTask
   */
  public FetchTask getFetchTask() {
    return fetchTask;
  }

  /**
   * @param fetchTask
   *          the fetchTask to set
   */
  public void setFetchTask(FetchTask fetchTask) {
    this.fetchTask = fetchTask;
  }

  protected void reset() {
    rootTasks = new ArrayList<Task<? extends Serializable>>();
  }

  public static String stripQuotes(String val) {
    return PlanUtils.stripQuotes(val);
  }

  public static String charSetString(String charSetName, String charSetString)
      throws SemanticException {
    try {
      // The character set name starts with a _, so strip that
      charSetName = charSetName.substring(1);
      if (charSetString.charAt(0) == '\'') {
        return new String(unescapeSQLString(charSetString).getBytes(),
            charSetName);
      } else // hex input is also supported
      {
        assert charSetString.charAt(0) == '0';
        assert charSetString.charAt(1) == 'x';
        charSetString = charSetString.substring(2);

        byte[] bArray = new byte[charSetString.length() / 2];
        int j = 0;
        for (int i = 0; i < charSetString.length(); i += 2) {
          int val = Character.digit(charSetString.charAt(i), 16) * 16
              + Character.digit(charSetString.charAt(i + 1), 16);
          if (val > 127) {
            val = val - 256;
          }
          bArray[j++] = (byte)val;
        }

        String res = new String(bArray, charSetName);
        return res;
      }
    } catch (UnsupportedEncodingException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * Get dequoted name from a table/column node.
   * @param tableOrColumnNode the table or column node
   * @return for table node, db.tab or tab. for column node column.
   */
  public static String getUnescapedName(ASTNode tableOrColumnNode) {
    return getUnescapedName(tableOrColumnNode, null);
  }

  public static String getUnescapedName(ASTNode tableOrColumnNode, String currentDatabase) {
    if (tableOrColumnNode.getToken().getType() == HiveParser.TOK_TABNAME) {
      // table node
      if (tableOrColumnNode.getChildCount() == 2) {
        String dbName = unescapeIdentifier(tableOrColumnNode.getChild(0).getText());
        String tableName = unescapeIdentifier(tableOrColumnNode.getChild(1).getText());
        return dbName + "." + tableName;
      }else if(tableOrColumnNode.getChildCount() == 3) {//added by zjw for datacenter
        zlog.info("---zjw define datacenter name");
        String dcName = unescapeIdentifier(tableOrColumnNode.getChild(0).getText());
        String dbName = unescapeIdentifier(tableOrColumnNode.getChild(1).getText());
        String tableName = unescapeIdentifier(tableOrColumnNode.getChild(2).getText());
        return dcName + "." +dbName + "." + tableName;
      }
      String tableName = unescapeIdentifier(tableOrColumnNode.getChild(0).getText());
      if (currentDatabase != null) {
        return currentDatabase + "." + tableName;
      }
      return tableName;
    }
    // column node
    return unescapeIdentifier(tableOrColumnNode.getText());
  }

  /**
   * Get the unqualified name from a table node.
   *
   * This method works for table names qualified with their schema (e.g., "db.table")
   * and table names without schema qualification. In both cases, it returns
   * the table name without the schema.
   *
   * @param node the table node
   * @return the table name without schema qualification
   *         (i.e., if name is "db.table" or "table", returns "table")
   */
  public static String getUnescapedUnqualifiedTableName(ASTNode node) {
    assert node.getChildCount() <= 2;

    if (node.getChildCount() == 2) {
      node = (ASTNode) node.getChild(1);
    }

    return getUnescapedName(node);
  }


  /**
   * Remove the encapsulating "`" pair from the identifier. We allow users to
   * use "`" to escape identifier for table names, column names and aliases, in
   * case that coincide with Hive language keywords.
   */
  public static String unescapeIdentifier(String val) {
    if (val == null) {
      return null;
    }
    if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
      val = val.substring(1, val.length() - 1);
    }
    return val;
  }

  /**
   * Converts parsed key/value properties pairs into a map.
   *
   * @param prop ASTNode parent of the key/value pairs
   *
   * @param mapProp property map which receives the mappings
   */
  public static void readProps(
    ASTNode prop, Map<String, String> mapProp) {

    for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
      String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
          .getText());
      String value = unescapeSQLString(prop.getChild(propChild).getChild(1)
          .getText());
      if (value == null){
        value = "";
      }
      mapProp.put(key, value);
    }
  }
  public static void readNodes(
      ASTNode node, Set<String> setNode) {
      for (int nodeChild = 0; nodeChild < node.getChildCount(); nodeChild++) {
        String value = unescapeSQLString(node.getChild(nodeChild).getText());
        setNode.add(value);
      }
  }

  @SuppressWarnings("nls")
  public static String unescapeSQLString(String b) {

    Character enclosure = null;

    // Some of the strings can be passed in as unicode. For example, the
    // delimiter can be passed in as \002 - So, we first check if the
    // string is a unicode number, else go back to the old behavior
    StringBuilder sb = new StringBuilder(b.length());
    for (int i = 0; i < b.length(); i++) {

      char currentChar = b.charAt(i);
      if (enclosure == null) {
        if (currentChar == '\'' || b.charAt(i) == '\"') {
          enclosure = currentChar;
        }
        // ignore all other chars outside the enclosure
        continue;
      }

      if (enclosure.equals(currentChar)) {
        enclosure = null;
        continue;
      }

      if (currentChar == '\\' && (i + 4 < b.length())) {
        char i1 = b.charAt(i + 1);
        char i2 = b.charAt(i + 2);
        char i3 = b.charAt(i + 3);
        if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7')
            && (i3 >= '0' && i3 <= '7')) {
          byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
          byte[] bValArr = new byte[1];
          bValArr[0] = bVal;
          String tmp = new String(bValArr);
          sb.append(tmp);
          i += 3;
          continue;
        }
      }

      if (currentChar == '\\' && (i + 2 < b.length())) {
        char n = b.charAt(i + 1);
        switch (n) {
        case '0':
          sb.append("\0");
          break;
        case '\'':
          sb.append("'");
          break;
        case '"':
          sb.append("\"");
          break;
        case 'b':
          sb.append("\b");
          break;
        case 'n':
          sb.append("\n");
          break;
        case 'r':
          sb.append("\r");
          break;
        case 't':
          sb.append("\t");
          break;
        case 'Z':
          sb.append("\u001A");
          break;
        case '\\':
          sb.append("\\");
          break;
        // The following 2 lines are exactly what MySQL does
        case '%':
          sb.append("\\%");
          break;
        case '_':
          sb.append("\\_");
          break;
        default:
          sb.append(n);
        }
        i++;
      } else {
        sb.append(currentChar);
      }
    }
    return sb.toString();
  }

  public HashSet<ReadEntity> getInputs() {
    return inputs;
  }

  public HashSet<WriteEntity> getOutputs() {
    return outputs;
  }

  /**
   * @return the schema for the fields which will be produced
   * when the statement is executed, or null if not known
   */
  public List<FieldSchema> getResultSchema() {
    return null;
  }

  protected List<FieldSchema> getColumns(ASTNode ast) throws SemanticException {
    return getColumns(ast, true);
  }

  protected void handleGenericFileFormat(ASTNode node) throws SemanticException{

  ASTNode child = (ASTNode)node.getChild(0);
  throw new SemanticException("Unrecognized file format in STORED AS clause:"+
         " "+ (child == null ? "" : child.getText()));
  }

  /**
   * Get the list of FieldSchema out of the ASTNode.
   *
   * added by zjw:we re-use lowerCase indicate part cols retrival.
   */
  public static List<FieldSchema> getColumns(ASTNode ast, boolean lowerCase) throws SemanticException {
    List<FieldSchema> colList = new ArrayList<FieldSchema>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      FieldSchema col = new FieldSchema();
      ASTNode child = (ASTNode) ast.getChild(i);


      String name = child.getChild(0).getText();
      if(lowerCase) {
        name = name.toLowerCase();
      }
      // child 0 is the name of the column
      col.setName(unescapeIdentifier(name));
      // child 1 is the type of the column
      ASTNode typeChild = (ASTNode) (child.getChild(1));
      col.setType(getTypeStringFromAST(typeChild));

      // child 2 is the optional comment of the column
      if (child.getChildCount() == 3) {
        col.setComment(unescapeSQLString(child.getChild(2).getText()));
      }
      colList.add(col);

    }
    return colList;
  }

  /**
   *目前只支持二级分区
   *^(TOK_PARTITIONED_BY $partParamList
    tableSubPartition?
    partitionTemplate?)
   *
   * create table abc( a int,b string) partitioned by list(a) partition pa values(1,2);
   *
TOK_TABLEPARTCOLS--text:TOK_TABLEPARTCOLS--tokenType:141
  TOK_FUNCTION--text:TOK_FUNCTION--tokenType:21----------------------<list(a)>
TOK_PARTITION_EXPER--text:TOK_PARTITION_EXPER--tokenType:255
  TOK_PARTITION--text:TOK_PARTITION--tokenType:257-------------------<pa values(1,2)>



   * @param colList
   * @return 返回一级分区的列定义
   * added by zjw for 2-level partition analyze
   */

  public static List<FieldSchema> analyzePartitionClause(ASTNode ast, PartitionDefinition pd) throws SemanticException {
      List<FieldSchema> colList = new ArrayList<FieldSchema>();
      PartitionDefinition global_sub_pd = null;
    //table partition columns analyze
      zlog.warn("Ast tree:"+ast.toStringTree());
      int partChildNum = ast.getChildCount();
      for (int p = 0; p < partChildNum; p++) {//anyalyze partition by

        ASTNode p_child = (ASTNode) ast.getChild(p);
        zlog.warn("in part columns-111,tree:"+p_child.toString()
            +"--text:"+p_child.getText()+"--tokenType:"+p_child.getToken().getType());
        switch(p_child.getToken().getType()){
        case  HiveParser.TOK_TABLEPARTCOLS://part function:hash/list/range/interval
          getPartitionType( p_child,colList, pd);
          break;
        case  HiveParser.TOK_SPLIT_EXPER:
        case  HiveParser.TOK_PARTITION_EXPER://一级分区定义，子分区定义也在此分支中处理
          zlog.warn("TOK_PARTITION_EXPER,tree+++++");
          List<PartitionDefinition> parts = getPartitionDef(p_child,global_sub_pd);
          pd.setPartitions(parts);

          break;
        case  HiveParser.TOK_SUBSPLITED_BY:
        case  HiveParser.TOK_SUBPARTITIONED_BY://直接跟在一级分区定义后，本子分区定义会直接传递给一级分区的所有分区
          global_sub_pd = new PartitionDefinition();
          global_sub_pd.setTableName(pd.getTableName());
          global_sub_pd.getPi().setP_level(2);//设为2级分区
          List<FieldSchema> subPartCol = analyzePartitionClause( p_child,  global_sub_pd);//递归调用，获取子分区定义
//          List<SubPartitionFieldSchema> subPartFieldList =
//              PartitionFactory.toSubPartitionFieldSchemaList(subPartCol);
          colList.addAll(subPartCol);
          PartitionFactory.createSubPartition(colList,global_sub_pd,true,null);
//          assert(colList.size() == 1);

          break;
        case  HiveParser.TOK_SUBSPLIT_EXPER:
        case  HiveParser.TOK_SUBPARTITION_EXPER://用递归调用时，本分支仅在第二层调用，不用递归应放在TOK_SUBPARTITIONED_BY里面解析
          zlog.warn("TOK_SUBPARTITION_EXPER,tree-----");
          List<PartitionDefinition> sub_parts = getPartitionDef(p_child,global_sub_pd);
          pd.setPartitions(sub_parts);
          break;
        default:
          assert(false);
        }

      }
//      FieldSchema fieldSchema = pd.toMFieldSchema();

      return colList;
  }


  public static List<FieldSchema> analyzeFielSplitClause(ASTNode ast, PartitionDefinition pd) throws SemanticException {
    List<FieldSchema> colList = new ArrayList<FieldSchema>();
    PartitionDefinition global_sub_pd = null;
  //table partition columns analyze
    zlog.warn("Ast tree:"+ast.toStringTree());
    int partChildNum = ast.getChildCount();
    for (int p = 0; p < partChildNum; p++) {//anyalyze partition by

      ASTNode p_child = (ASTNode) ast.getChild(p);
      zlog.warn("in part columns-111,tree:"+p_child.toString()
          +"--text:"+p_child.getText()+"--tokenType:"+p_child.getToken().getType());
      switch(p_child.getToken().getType()){
      case  HiveParser.TOK_TABLEPARTCOLS://part function:hash/list/range/interval
        getPartitionType( p_child,colList, pd);
        break;
      case  HiveParser.TOK_SPLIT_EXPER://一级分区定义，子分区定义也在此分支中处理
        zlog.warn("TOK_PARTITION_EXPER,tree+++++");
        List<PartitionDefinition> parts = getPartitionDef(p_child,global_sub_pd);
        pd.setPartitions(parts);

        break;
      case  HiveParser.TOK_SUBSPLITED_BY://直接跟在一级分区定义后，本子分区定义会直接传递给一级分区的所有分区
        global_sub_pd = new PartitionDefinition();
        global_sub_pd.setTableName(pd.getTableName());
        global_sub_pd.getPi().setP_level(2);//设为2级分区
        List<FieldSchema> subPartCol = analyzePartitionClause( p_child,  global_sub_pd);//递归调用，获取子分区定义
//        List<SubPartitionFieldSchema> subPartFieldList =
//            PartitionFactory.toSubPartitionFieldSchemaList(subPartCol);
        colList.addAll(subPartCol);
        PartitionFactory.createSubPartition(colList,global_sub_pd,true,null);
//        assert(colList.size() == 1);

        break;
      case  HiveParser.TOK_SUBSPLIT_EXPER://用递归调用时，本分支仅在第二层调用，不用递归应放在TOK_SUBPARTITIONED_BY里面解析
        zlog.warn("TOK_SUBPARTITION_EXPER,tree-----");
        List<PartitionDefinition> sub_parts = getPartitionDef(p_child,global_sub_pd);
        pd.setPartitions(sub_parts);
        break;
      default:
        assert(false);
      }

    }
//    FieldSchema fieldSchema = pd.toMFieldSchema();

    return colList;
}

  /**
   * 解析对分区方法的定义
   * @param p_child
   * @param colList
   * @param pd
   * @throws SemanticException
   */
  protected static void getPartitionType(ASTNode p_child,List<FieldSchema> colList,PartitionDefinition pd) throws SemanticException{
    ASTNode func_child = (ASTNode) p_child.getChild(0);
    String func_name = func_child.getChild(0).getText().toLowerCase();
    if(!PartitionFactory.PartitionType.validate(func_name)){
      throw new SemanticException("Partition type:"+func_name+" is not suppored.");
    }else{
      pd.getPi().setP_type(PartitionFactory.PartitionType.valueOf(func_name));
//      pd.setPartitionLevel(1);
    }
    zlog.warn("p_child,tree:"+func_child.toStringTree()+"--text:"+func_child.getText()
        +"--tokenType:"+func_child.getChildCount());
    int paraNum = func_child.getChildCount();

    for (int i = 1; i < paraNum; i++) {//anyalyze partition function params
      ASTNode func_para = (ASTNode) func_child.getChild(i);

      if(i==1 && func_para.getToken().getType() !=  HiveParser.TOK_TABLE_OR_COL){
        throw new SemanticException("Partition/Filesplit first parameter must be a columne name,please check if column name quotaed by ' or \".");
      }

      if(func_para.getToken().getType() ==  HiveParser.TOK_TABLE_OR_COL){
        FieldSchema col = new FieldSchema();
        col.setName(func_para.getChild(0).getText());//@todo  add validate partcol in columns
//        colList.add(col);//@todo remove this
        pd.getPi().setP_col(col.getName());
      }else{
        if(pd.getPi().getP_type()== PartitionFactory.PartitionType.hash && i==2){
          pd.getPi().setP_num(Integer.parseInt(func_child.getChild(i).getText()));
        }
        pd.getPi().getArgs().add(func_child.getChild(i).getText());
      }
      zlog.warn("func_para,tree:"+func_para.toStringTree()+"--text:"+func_para.getText()
        +"--getChildCount:"+func_para.getChildCount());
    }

    colList.add(pd.toFieldSchema());//@todo remove this
  }

  /**
   * 解析对各分区的定义
   * @param p_child
   * @return
   */
  protected static List<PartitionDefinition> getPartitionDef(ASTNode p_child,PartitionDefinition global_sub_pd){
    List<PartitionDefinition> parts = new ArrayList<PartitionDefinition>();
    String last_part_min_value = PartitionConstants.MINVALUE;
    String last_part_max_value = PartitionConstants.MAXVALUE;

    zlog.warn("getPartitionDef,tree:"+p_child.toStringTree()+"--text:"+p_child.getText()
        +"--getChildCount:"+p_child.getChildCount());
    int ptempNum = p_child.getChildCount();
    for (int t = 0; t < ptempNum; t++) {//anyalyze partition template
      PartitionDefinition partition = new PartitionDefinition();
      ASTNode child = (ASTNode) p_child.getChild(t);
      ASTNode part_name = (ASTNode) child.getChild(0);
      if(child.getChildCount()>=2){
        ASTNode part_para = (ASTNode) child.getChild(1);
        partition.setPart_name(part_name.getText());

        if(global_sub_pd != null){
          partition.setTableName(global_sub_pd.getTableName());
          partition.getPi().setP_col(global_sub_pd.getPi().getP_col());
          partition.getPi().setP_type(global_sub_pd.getPi().getP_type());
          partition.getPi().setP_level(global_sub_pd.getPi().getP_level());
          partition.getPi().setP_num(global_sub_pd.getPi().getP_num());
          partition.getPi().setP_order(global_sub_pd.getPi().getP_order());
          partition.getPi().setP_version(global_sub_pd.getPi().getP_version());
          partition.getPi().setArgs(global_sub_pd.getPi().getArgs());
        }
        zlog.warn("value_para,tree:"+part_para.toStringTree()+"--text:"+part_para.getText());
        switch(part_para.getToken().getType()){
          case  HiveParser.TOK_VALUES:
            ASTNode paras = (ASTNode) part_para.getChild(0);
            for (int i = 0; i < paras.getChildCount(); i++) {//anyalyze partition function params
              ASTNode para_value = (ASTNode) paras.getChild(i);
              partition.getValues().add(para_value.getText());
            }
            break;
          case  HiveParser.TOK_VALUES_LESS:
            String value_i = part_para.getChild(0).getText();
            partition.getValues().add(last_part_min_value);
            partition.getValues().add(value_i);
            last_part_min_value = value_i;
            break;
          case  HiveParser.TOK_VALUES_GREATER:
            String value_a = part_para.getChild(0).getText();
            partition.getValues().add(value_a);
            partition.getValues().add(last_part_max_value);
            last_part_max_value = value_a;
            break;
          default:
            assert(false);
        }
      }
      if(child.getChildCount()>=3){//subpartition clause
        ASTNode sub_part_temp = (ASTNode) child.getChild(2);
        List<PartitionDefinition> sub_parts = getPartitionDef(sub_part_temp,null);//递归调用
        partition.setPartitions(sub_parts);
      }else{
        if(global_sub_pd != null){
          partition.cloneToSubpartitions(global_sub_pd.getPartitions());
        }
      }
      parts.add(partition);

      zlog.warn("partition,tree:"+child.toStringTree()+"--text:"+child.getText()
        +"--getChildCount:"+child.getChildCount());
    }
    return parts;
  }

  protected List<String> getColumnNames(ASTNode ast) {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      colList.add(unescapeIdentifier(child.getText()).toLowerCase());
    }
    return colList;
  }

  protected List<Order> getColumnNamesOrder(ASTNode ast) {
    List<Order> colList = new ArrayList<Order>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      if (child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
        colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()).toLowerCase(),
            HIVE_COLUMN_ORDER_ASC));
      } else {
        colList.add(new Order(unescapeIdentifier(child.getChild(0).getText()).toLowerCase(),
            HIVE_COLUMN_ORDER_DESC));
      }
    }
    return colList;
  }

  protected static String getTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    switch (typeNode.getType()) {
    case HiveParser.TOK_LIST:
      return serdeConstants.LIST_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ">";
    case HiveParser.TOK_MAP:
      return serdeConstants.MAP_TYPE_NAME + "<"
          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ","
          + getTypeStringFromAST((ASTNode) typeNode.getChild(1)) + ">";
    case HiveParser.TOK_STRUCT:
      return getStructTypeStringFromAST(typeNode);
    case HiveParser.TOK_UNIONTYPE:
      return getUnionTypeStringFromAST(typeNode);
    default:
      return DDLSemanticAnalyzer.getTypeName(typeNode.getType());
    }
  }

  private static String getStructTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = serdeConstants.STRUCT_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0) {
      throw new SemanticException("empty struct not allowed.");
    }
    StringBuilder buffer = new StringBuilder(typeStr);
    for (int i = 0; i < children; i++) {
      ASTNode child = (ASTNode) typeNode.getChild(i);
      buffer.append(unescapeIdentifier(child.getChild(0).getText())).append(":");
      buffer.append(getTypeStringFromAST((ASTNode) child.getChild(1)));
      if (i < children - 1) {
        buffer.append(",");
      }
    }

    buffer.append(">");
    return buffer.toString();
  }

  private static String getUnionTypeStringFromAST(ASTNode typeNode)
      throws SemanticException {
    String typeStr = serdeConstants.UNION_TYPE_NAME + "<";
    typeNode = (ASTNode) typeNode.getChild(0);
    int children = typeNode.getChildCount();
    if (children <= 0) {
      throw new SemanticException("empty union not allowed.");
    }
    StringBuilder buffer = new StringBuilder(typeStr);
    for (int i = 0; i < children; i++) {
      buffer.append(getTypeStringFromAST((ASTNode) typeNode.getChild(i)));
      if (i < children - 1) {
        buffer.append(",");
      }
    }
    buffer.append(">");
    typeStr = buffer.toString();
    return typeStr;
  }

  /**
   * tableSpec.
   *
   */
  public static class tableSpec {
    public String tableName;
    public Table tableHandle;
    public Map<String, String> partSpec; // has to use LinkedHashMap to enforce order
    public Partition partHandle;
    public int numDynParts; // number of dynamic partition columns
    public List<Partition> partitions; // involved partitions in TableScanOperator/FileSinkOperator
    public static enum SpecType {TABLE_ONLY, STATIC_PARTITION, DYNAMIC_PARTITION};
    public SpecType specType;

    public tableSpec(Hive db, HiveConf conf, ASTNode ast)
        throws SemanticException {
      this(db, conf, ast, true, false);
    }

    public tableSpec(Hive db, HiveConf conf, ASTNode ast,
        boolean allowDynamicPartitionsSpec, boolean allowPartialPartitionsSpec)
        throws SemanticException {

      assert (ast.getToken().getType() == HiveParser.TOK_TAB
          || ast.getToken().getType() == HiveParser.TOK_TABLE_PARTITION
          || ast.getToken().getType() == HiveParser.TOK_TABTYPE
          || ast.getToken().getType() == HiveParser.TOK_CREATETABLE);
      int childIndex = 0;
      numDynParts = 0;

      try {
        // get table metadata
        tableName = getUnescapedName((ASTNode)ast.getChild(0));
        boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
        if (testMode) {
          tableName = conf.getVar(HiveConf.ConfVars.HIVETESTMODEPREFIX)
              + tableName;
        }
        if (ast.getToken().getType() != HiveParser.TOK_CREATETABLE) {
          tableHandle = db.getTable(tableName);
        }
      } catch (InvalidTableException ite) {
        throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(ast
            .getChild(0)), ite);
      } catch (HiveException e) {
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg(ast
            .getChild(childIndex), e.getMessage()), e);
      }

      // get partition metadata if partition specified
      if (ast.getChildCount() == 2 && ast.getToken().getType() != HiveParser.TOK_CREATETABLE) {
        childIndex = 1;
        ASTNode partspec = (ASTNode) ast.getChild(1);
        partitions = new ArrayList<Partition>();
        // partSpec is a mapping from partition column name to its value.
        partSpec = new LinkedHashMap<String, String>(partspec.getChildCount());
        for (int i = 0; i < partspec.getChildCount(); ++i) {
          ASTNode partspec_val = (ASTNode) partspec.getChild(i);
          String val = null;
          String colName = unescapeIdentifier(partspec_val.getChild(0).getText().toLowerCase());
          if (partspec_val.getChildCount() < 2) { // DP in the form of T partition (ds, hr)
            if (allowDynamicPartitionsSpec) {
              ++numDynParts;
            } else {
              throw new SemanticException(ErrorMsg.INVALID_PARTITION
                                                       .getMsg(" - Dynamic partitions not allowed"));
            }
          } else { // in the form of T partition (ds="2010-03-03")
            val = stripQuotes(partspec_val.getChild(1).getText());
          }
          partSpec.put(colName, val);
        }

        // check if the columns specified in the partition() clause are actually partition columns
        Utilities.validatePartSpec(tableHandle, partSpec);

        // check if the partition spec is valid
        if (numDynParts > 0) {
          List<FieldSchema> parts = tableHandle.getPartitionKeys();
          int numStaPart = parts.size() - numDynParts;
          if (numStaPart == 0 &&
              conf.getVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE).equalsIgnoreCase("strict")) {
            throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg());
          }

          // check the partitions in partSpec be the same as defined in table schema
          if (partSpec.keySet().size() != parts.size()) {
            ErrorPartSpec(partSpec, parts);
          }
          Iterator<String> itrPsKeys = partSpec.keySet().iterator();
          for (FieldSchema fs: parts) {
            if (!itrPsKeys.next().toLowerCase().equals(fs.getName().toLowerCase())) {
              ErrorPartSpec(partSpec, parts);
            }
          }

          // check if static partition appear after dynamic partitions
          for (FieldSchema fs: parts) {
            if (partSpec.get(fs.getName().toLowerCase()) == null) {
              if (numStaPart > 0) { // found a DP, but there exists ST as subpartition
                throw new SemanticException(
                    ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg(ast.getChild(childIndex)));
              }
              break;
            } else {
              --numStaPart;
            }
          }
          partHandle = null;
          specType = SpecType.DYNAMIC_PARTITION;
        } else {
          try {
            if (allowPartialPartitionsSpec) {
              partitions = db.getPartitions(tableHandle, partSpec);
            } else {
              // this doesn't create partition.
              partHandle = db.getPartition(tableHandle, partSpec, false);
              if (partHandle == null) {
                // if partSpec doesn't exists in DB, return a delegate one
                // and the actual partition is created in MoveTask
                partHandle = new Partition(tableHandle, partSpec, null);
              } else {
                partitions.add(partHandle);
              }
            }
          } catch (HiveException e) {
            throw new SemanticException(
                ErrorMsg.INVALID_PARTITION.getMsg(ast.getChild(childIndex)), e);
          }
          specType = SpecType.STATIC_PARTITION;
        }
      } else {
        specType = SpecType.TABLE_ONLY;
      }
    }

    public Map<String, String> getPartSpec() {
      return this.partSpec;
    }

    public void setPartSpec(Map<String, String> partSpec) {
      this.partSpec = partSpec;
    }

    @Override
    public String toString() {
      if (partHandle != null) {
        return partHandle.toString();
      } else {
        return tableHandle.toString();
      }
    }

  }

  /**
   * Gets the lineage information.
   *
   * @return LineageInfo associated with the query.
   */
  public LineageInfo getLineageInfo() {
    return linfo;
  }

  /**
   * Sets the lineage information.
   *
   * @param linfo The LineageInfo structure that is set in the optimization phase.
   */
  public void setLineageInfo(LineageInfo linfo) {
    this.linfo = linfo;
  }

  /**
   * Gets the table access information.
   *
   * @return TableAccessInfo associated with the query.
   */
  public TableAccessInfo getTableAccessInfo() {
    return tableAccessInfo;
  }

  /**
   * Sets the table access information.
   *
   * @param taInfo The TableAccessInfo structure that is set in the optimization phase.
   */
  public void setTableAccessInfo(TableAccessInfo tableAccessInfo) {
    this.tableAccessInfo = tableAccessInfo;
  }

  protected HashMap<String, String> extractPartitionSpecs(Tree partspec)
      throws SemanticException {
    HashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i = 0; i < partspec.getChildCount(); ++i) {
      CommonTree partspec_val = (CommonTree) partspec.getChild(i);
      String val = stripQuotes(partspec_val.getChild(1).getText());
      partSpec.put(partspec_val.getChild(0).getText().toLowerCase(), val);
    }
    return partSpec;
  }

  /**
   * Checks if given specification is proper specification for prefix of
   * partition cols, for table partitioned by ds, hr, min valid ones are
   * (ds='2008-04-08'), (ds='2008-04-08', hr='12'), (ds='2008-04-08', hr='12', min='30')
   * invalid one is for example (ds='2008-04-08', min='30')
   * @param spec specification key-value map
   * @return true if the specification is prefix; never returns false, but throws
   * @throws HiveException
   */
  final public boolean isValidPrefixSpec(Table tTable, Map<String, String> spec)
 throws HiveException {

    // TODO - types need to be checked.
    List<FieldSchema> partCols = tTable.getPartitionKeys();
    if (partCols == null || (partCols.size() == 0)) {
      if (spec != null) {
        throw new HiveException(
            "table is not partitioned but partition spec exists: "
                + spec);
      } else {
        return true;
      }
    }

    if (spec == null) {
      throw new HiveException("partition spec is not specified");
    }

    Iterator<String> itrPsKeys = spec.keySet().iterator();
    for (FieldSchema fs: partCols) {
      if(!itrPsKeys.hasNext()) {
        break;
      }
      if (!itrPsKeys.next().toLowerCase().equals(
              fs.getName().toLowerCase())) {
        ErrorPartSpec(spec, partCols);
      }
    }

    if(itrPsKeys.hasNext()) {
      ErrorPartSpec(spec, partCols);
    }

    return true;
  }

  private static void ErrorPartSpec(Map<String, String> partSpec,
      List<FieldSchema> parts) throws SemanticException {
    StringBuilder sb =
        new StringBuilder(
            "Partition columns in the table schema are: (");
    for (FieldSchema fs : parts) {
      sb.append(fs.getName()).append(", ");
    }
    sb.setLength(sb.length() - 2); // remove the last ", "
    sb.append("), while the partitions specified in the query are: (");

    Iterator<String> itrPsKeys = partSpec.keySet().iterator();
    while (itrPsKeys.hasNext()) {
      sb.append(itrPsKeys.next()).append(", ");
    }
    sb.setLength(sb.length() - 2); // remove the last ", "
    sb.append(").");
    throw new SemanticException(ErrorMsg.PARTSPEC_DIFFER_FROM_SCHEMA
        .getMsg(sb.toString()));
  }

  public Hive getDb() {
    return db;
  }

  public QueryProperties getQueryProperties() {
    return queryProperties;
  }

  /**
   * Given a ASTNode, return list of values.
   *
   * use case:
   *   create table xyz list bucketed (col1) with skew (1,2,5)
   *   AST Node is for (1,2,5)
   * @param ast
   * @return
   */
  protected List<String> getSkewedValueFromASTNode(ASTNode ast) {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      colList.add(stripQuotes(child.getText()).toLowerCase());
    }
    return colList;
  }

  /**
   * Retrieve skewed values from ASTNode.
   *
   * @param node
   * @return
   * @throws SemanticException
   */
  protected List<String> getSkewedValuesFromASTNode(Node node) throws SemanticException {
    List<String> result = null;
    Tree leafVNode = ((ASTNode) node).getChild(0);
    if (leafVNode == null) {
      throw new SemanticException(
          ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
    } else {
      ASTNode lVAstNode = (ASTNode) leafVNode;
      if (lVAstNode.getToken().getType() != HiveParser.TOK_TABCOLVALUE) {
        throw new SemanticException(
            ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
      } else {
        result = new ArrayList<String>(getSkewedValueFromASTNode(lVAstNode));
      }
    }
    return result;
  }

  /**
   * Analyze list bucket column names
   *
   * @param skewedColNames
   * @param child
   * @return
   * @throws SemanticException
   */
  protected List<String> analyzeSkewedTablDDLColNames(List<String> skewedColNames, ASTNode child)
      throws SemanticException {
  Tree nNode = child.getChild(0);
    if (nNode == null) {
      throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_NAME.getMsg());
    } else {
      ASTNode nAstNode = (ASTNode) nNode;
      if (nAstNode.getToken().getType() != HiveParser.TOK_TABCOLNAME) {
        throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_NAME.getMsg());
      } else {
        skewedColNames = getColumnNames(nAstNode);
      }
    }
    return skewedColNames;
  }

  /**
   * Handle skewed values in DDL.
   *
   * It can be used by both skewed by ... on () and set skewed location ().
   *
   * @param skewedValues
   * @param child
   * @throws SemanticException
   */
  protected void analyzeDDLSkewedValues(List<List<String>> skewedValues, ASTNode child)
      throws SemanticException {
  Tree vNode = child.getChild(1);
    if (vNode == null) {
      throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
    }
    ASTNode vAstNode = (ASTNode) vNode;
    switch (vAstNode.getToken().getType()) {
      case HiveParser.TOK_TABCOLVALUE:
        for (String str : getSkewedValueFromASTNode(vAstNode)) {
          List<String> sList = new ArrayList<String>(Arrays.asList(str));
          skewedValues.add(sList);
        }
        break;
      case HiveParser.TOK_TABCOLVALUE_PAIR:
        ArrayList<Node> vLNodes = vAstNode.getChildren();
        for (Node node : vLNodes) {
          if ( ((ASTNode) node).getToken().getType() != HiveParser.TOK_TABCOLVALUES) {
            throw new SemanticException(
                ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
          } else {
            skewedValues.add(getSkewedValuesFromASTNode(node));
          }
        }
        break;
      default:
        break;
    }
  }

  /**
   * process stored as directories
   *
   * @param child
   * @return
   */
  protected boolean analyzeStoredAdDirs(ASTNode child) {
    boolean storedAsDirs = false;
    if ((child.getChildCount() == 3)
        && (((ASTNode) child.getChild(2)).getToken().getType()
            == HiveParser.TOK_STOREDASDIRS)) {
      storedAsDirs = true;
    }
    return storedAsDirs;
  }
}
