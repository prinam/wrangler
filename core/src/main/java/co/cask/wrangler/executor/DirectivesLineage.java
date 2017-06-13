/*
 * Copyright © 2016-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.executor;

import co.cask.wrangler.api.Step;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * DataType for storing directives for each column
 * Instance of this type to be sent to platform through API
 */

public class DirectivesLineage {
    private static int NOT_REAL = -1;
    private static int NEW_DIR = 0;

    final long startTime;
    final String programName;
    private String workspaceName; // Not final
    private Map<String, ColumnDirective> lineage;

    public DirectivesLineage(String[] columnNames) {
        this.startTime = System.currentTimeMillis();
        this.programName = "wrangler";
        this.workspaceName = "PLEASE_CHANGE_THIS"; /*getWorkspaceName()*/;
        if(columnNames == null)
            this.lineage = new HashMap<>();
        else {
            this.lineage = new HashMap<>(columnNames.length);
            for (int i = 0; i < columnNames.length; ++i)
                lineage.put(columnNames[i], new ColumnDirective(columnNames[i], i));
        }
    }

    public String getWorkspaceName() { return this.workspaceName; }

    public void setWorkspaceName(String newName) { this.workspaceName = newName }

    private void insert(String column, Step currStep) {
        ColumnDirective currDirective = lineage.get(column);
        if(currDirective.version == 0 || lineage.containsKey(column + " " + currDirective.version))
            currDirective.version++;
        currDirective.insertMetaStep(currStep);
    }

    private void insert(String column, MetaStep currStep) {
        ColumnDirective currDirective = lineage.get(column);
        if(currDirective.version == 0 || lineage.containsKey(column + " " + currDirective.version))
            currDirective.version++;
        currDirective.insertMetaStep(currStep);
    }

    private String branchIfNecessary(String column) {
        ColumnDirective currDirective = lineage.get(column);
        if(currDirective.version == 0)
            return null;
        if(lineage.containsKey(column + " " + currDirective.version))
            return column + " " + currDirective.version;
        ColumnDirective newDirective = new ColumnDirective(currDirective.steps);
        lineage.put(column + " " + currDirective.version, newDirective);
        return column + " " + currDirective.version;
    }

    /*
    public void parse(Step currStep) {
        switch(currStep.getClass().getSimpleName()) {
            case "setcolumns" // would need to get names form the classes and would require these to be at somehow more accessible not private maybe package public or something
        }
    }
    */

    public void parse(Step currStep, String directiveString, String[] cols) {
        switch (directiveString) {
            // set columns <name1, name2, ...>
            case "setcolumns": {
                int value;
                for(String key : lineage.keySet()) {
                    value = lineage.get(key).colNum;
                    if(value != NOT_REAL) {
                        lineage.put(cols[value], lineage.remove(key));
                        lineage.get(cols[value]).version = NEW_DIR;
                        insert(key, currStep);
                    }
                }
            }
            break;

            // rename <old> <new>
            case "rename": {
                lineage.put(cols[1], lineage.remove(cols[0]));
                lineage.get(cols[1]).version = NEW_DIR;
                insert(cols[1], currStep);
            }
            break;

            // drop <column>[,<column>]
            case "drop": {
                int num;
                for(String key : cols) {
                    num = lineage.get(key).colNum;
                    lineage.remove(key);
                    for(String key2 : lineage.keySet()) {
                        if(lineage.get(key2).colNum > num)
                            lineage.get(key2).colNum--;
                    }
                }
            }
            break;

            // merge <first> <second> <new-column> <seperator>
            case "merge": {
                String newNameCol1, newNameCol2;
                HashMap<String, String> branches = new HashMap<>(2);
                newNameCol1 = branchIfNecessary(cols[0]);
                newNameCol2 = branchIfNecessary(cols[1]);
                branches.put(cols[0], newNameCol1);
                branches.put(cols[1], newNameCol2);
                MetaStep metaStep = new MetaStep(currStep, branches);
                insert(cols[2], metaStep);
            }
            break;
            default:
        }
    }

    public void parse(Step currStep, String directiveString, String column) {
        switch (directiveString) {
            // set format [csv|json] <delimiter> <skip empty lines>
            // TODO: This case
            case "setformat": {
            }
            break;

            // set column <column-name> <jexl-expression>
            case "setcolumn": {
                insert(column, currStep);
            }
            break;

            //set-type <column> <type>
            case "set-type": {
                insert(column, currStep);
            }
            break;

            // uppercase <col>
            case "uppercase": {
                insert(column, currStep);
            }
            break;

            // lowercase <col>
            case "lowercase": {
                insert(column, currStep);
            }
            break;

            // titlecase <col>
            case "titlecase": {
                insert(column, currStep);
            }
            break;

            // indexsplit <source> <start> <end> <destination>
            // DEPRECATED
            case "indexsplit": {
            }
            break;

            // split <source-column-name> <delimiter> <new-column-1> <new-column-2>
            // DEPRECATED
            case "split": {
            }
            break;

            // filter-row-if-matched <column> <regex>
            // DEPRECATED
            // not sure why????
            case "filter-row-if-matched": {
            }
            break;

            // filter-row-if-not-matched <column> <regex>
            case "filter-row-if-not-matched": {
                insert(column, currStep);
            }
            break;

            // filter-row-if-true  <condition>
            // DEPRECATED
            // not sure why????
            case "filter-row-if-true": {
            }
            break;

            // filter-row-if-false  <condition>
            case "filter-row-if-false": {
            }
            break;

            // set-variable <variable> <expression>
            case "set-variable": {
                insert(column, currStep);
            }
            break;

            // increment-variable <variable> <value> <expression>
            case "increment-variable": {
                insert(column, currStep);
            }
            break;

            // mask-number <column> <pattern>
            case "mask-number": {
                insert(column, currStep);
            }
            break;

            // mask-shuffle <column>
            case "mask-shuffle": {
                insert(column, currStep);
            }
            break;

            // format-date <column> <destination>
            case "format-date": {
                insert(column, currStep);
            }
            break;

            // format-unix-timestamp <column> <destination-format>
            // not sure why????
            case "format-unix-timestamp": {
                insert(column, currStep);
            }
            break;

            // quantize <source-column> <destination-column> <[range1:range2)=value>,[<range1:range2=value>]*
            case "quantize": {
            }
            break;

            // find-and-replace <column> <sed-script>
            case "find-and-replace" : {
                insert(column, currStep);
            }
            break;

            // parse-as-csv <column> <delimiter> [<header=true/false>]
            case "parse-as-csv" : {
            }
            break;

            // parse-as-json <column> [depth]
            case "parse-as-json" : {
            }
            break;

            // parse-as-avro <column> <schema-id> <json|binary> [version]
            case "parse-as-avro" : {
            }
            break;

            // parse-as-protobuf <column> <schema-id> <record-name> [version]
            case "parse-as-protobuf" : {
            }
            break;

            // json-path <source> <destination> <json-path>
            case "json-path" : {
            }
            break;

            // set-charset <column> <charset>
            case "set-charset" : {
                insert(column, currStep);
            }
            break;

            // invoke-http <url> <column>[,<column>] <header>[,<header>]
            case "invoke-http" : {
            }
            break;

            // set-record-delim <column> <delimiter> [<limit>]
            case "set-record-delim" : {
            }
            break;

            // parse-as-fixed-length <column> <widths> [<padding>]
            case "parse-as-fixed-length" : {
            }
            break;

            // split-to-rows <column> <separator>
            case "split-to-rows" : {
            }
            break;

            // split-to-columns <column> <regex>
            case "split-to-columns" : {
            }
            break;

            // parse-xml-to-json <column> [<depth>]
            case "parse-xml-to-json" : {
            }
            break;

            // parse-as-xml <column>
            case "parse-as-xml" : {
            }
            break;

            // parse-as-excel <column> <sheet number | sheet name>
            case "parse-as-excel" : {
            }
            break;

            // xpath <column> <destination> <xpath>
            case "xpath" : {
            }
            break;

            // xpath-array <column> <destination> <xpath>
            case "xpath-array" : {
            }
            break;

            // flatten <column>[,<column>,<column>,...]
            case "flatten" : {
            }
            break;

            // copy <source> <destination> [force]
            case "copy" : {
            }
            break;

            // fill-null-or-empty <column> <fixed value>
            case "fill-null-or-empty" : {
                insert(column, currStep);
            }
            break;

            // cut-character <source> <destination> <range|indexes>
            case "cut-character" : {
            }
            break;

            // generate-uuid <column>
            case "generate-uuid" : {
            }
            break;

            // url-encode <column>
            case "url-encode" : {
            }
            break;

            // url-decode <column>
            case "url-decode" : {
            }
            break;

            // parse-as-log <column> <format>
            case "parse-as-log" : {
            }
            break;

            // parse-as-date <column> [<timezone>]
            case "parse-as-date" : {
            }
            break;

            // parse-as-simple-date <column> <pattern>
            case "parse-as-simple-date" : {
            }
            break;

            // diff-date <column1> <column2> <destColumn>
            case "diff-date" : {
            }
            break;

            // keep <column>[,<column>]*
            case "keep" : {
            }
            break;

            // parse-as-hl7 <column> [<depth>]
            case "parse-as-hl7" : {
            }
            break;

            // split-email <column>
            case "split-email" : {
            }
            break;

            // swap <column1> <column2>
            case "swap" : {
            }
            break;

            // hash <column> <algorithm> [encode]
            case "hash" : {
            }
            break;

            // write-as-json <column>
            case "write-as-json-map" : {
            }
            break;

            // write-as-csv <column>
            case "write-as-csv" : {
            }
            break;

            // filter-rows-on condition-false <boolean-expression>
            case "filter-rows-on condition-false" : {
            }
            break;

            // filter-rows-on condition-true <boolean-expression>
            case "filter-rows-on condition-true" : {
            }
            break;

            // filter-rows-on empty-or-null-columns <column>[,<column>*]
            case "filter-rows-on empty-or-null-columns" : {
            }
            break;

            // filter-rows-on regex-match <regex>
            case "filter-rows-on regex-match" : {
            }
            break;

            // filter-rows-on regex-not-match <regex>
            case "filter-rows-on regex-not-match" : {
            }
            break;

            // parse-as-avro-file <column>
            case "parse-as-avro-file": {
            }
            break;

            // send-to-error <condition>
            case "send-to-error": {
            }
            break;

            // fail <condition>
            case "fail": {
            }
            break;

            // text-distance <method> <column1> <column2> <destination>
            case "text-distance" : {
            }
            break;

            // text-metric <method> <column1> <column2> <destination>
            case "text-metric" : {
            }
            break;

            // catalog-lookup ICD-9|ICD-10 <column>
            case "catalog-lookup" : {
            }
            break;

            // table-lookup <column> <table>
            case "table-lookup" : {
            }
            break;

            // stemming <column>
            case "stemming" : {
            }
            break;

            // columns <sed>
            case "columns-replace" : {
            }
            break;

            // extract-regex-groups <column> <regex>
            case "extract-regex-groups" : {
            }
            break;

            // split-url <column>
            case "split-url" : {
            }
            break;

            // cleanse-column-names
            case "cleanse-column-names" : {
            }
            break;

            // change-column-case <upper|lower|uppercase|lowercase>
            case "change-column-case" : {
            }
            break;

            // set-column <column> <expression>
            case "set-column" : {
            }
            break;

            // encode <base32|base64|hex> <column>
            case "encode" : {
            }
            break;

            // decode <base32|base64|hex> <column>
            case "decode" : {
            }
            break;

            //trim <column>
            case "trim": {
            }
            break;

            //ltrim <column>
            case "ltrim": {
            }
            break;

            //rtrim <column>
            case "rtrim": {
            }
            break;
            default:
        }
    }

//    Work in progress...
//    @Override
//    public String toString() {
//        StringBuilder temp = new StringBuilder("Workspace Name: " + workspaceName +
//                ", Program Name: " + programName + ", Start Time: " +
//                startTime + "\n" + "Column Directives:\n");
//        for(int i = 0; i < indexOfColumns.size(); ++i) {
//            String name = indexOfColumns.get(i);
//            temp.append("Column#: " + i + "Name: " + name + ", Original:");
//            temp.append(lineage.get(name));
//            temp.append("\n");
//        }
//        return String.valueOf(temp);
//    }

    private class ColumnDirective {
        final String ogName; // If null it is a new column
        int colNum; // Negative if not in workspace;
        int version;
        List<MetaStep> steps;

        ColumnDirective(List<MetaStep> stepsCopy) {
            this.ogName = null;
            this.version = NEW_DIR;
            this.colNum = NOT_REAL;
            this.steps = new ArrayList<>();
            this.steps.addAll(stepsCopy);
        }

        ColumnDirective(String ogName, int colNum) {
            this.ogName = ogName;
            this.version = NEW_DIR;
            this.colNum = colNum;
            this.steps = new ArrayList<>();
        }

        void insertMetaStep(Step s) {
            steps.add(NEW_DIR, new MetaStep(s));
        }

        void insertMetaStep(MetaStep s) {
            steps.add(NEW_DIR, s);
        }

//        @Override
//        public String toString() {
//            String temp;
//            if (ogName == null)
//                temp = "New column";
//            else
//                temp = ogName;
//            temp += ", " + steps;
//            return temp;
//        }
    }

    private class MetaStep {
        Step directive;
        Map<String, String> branchDirectives;

        MetaStep(Step directive) {
            this.directive = directive;
            branchDirectives = null;
        }

        MetaStep(Step directive, HashMap<String, String> branchDirectives) {
            this.directive = directive;
            this.branchDirectives = branchDirectives;
        }
    }
}
