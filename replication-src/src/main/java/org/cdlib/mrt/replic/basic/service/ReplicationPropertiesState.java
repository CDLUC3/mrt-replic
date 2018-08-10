/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/
package org.cdlib.mrt.replic.basic.service;

import java.util.Properties;
import java.util.ArrayList;
import java.util.List;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StateInf;

/**
 * State for multiple item entry results
 * @author  dloy
 */

public class ReplicationPropertiesState
        implements StateInf
{
    private static final String NAME = "ReplicationPropertiesState";
    private static final String MESSAGE = NAME + ": ";

    protected DateState reportDate = new DateState();
    protected Exception ex = null;
    protected ArrayList<Properties> rows = new ArrayList<Properties>();

    public ReplicationPropertiesState() { }
    
    public ReplicationPropertiesState(Properties [] rows)
    {
        setRows(rows);
    }

    public ReplicationPropertiesState(ArrayList<Properties> rows)
    {
        replaceEntries(rows);
    }


    public Properties getRow(int i)
    {
        if (i < 0) return null;
        if (i >= rows.size()) return null;
        return rows.get(i);
    }

    public void setRows(Properties [] rows) {
        if ((rows == null) || (rows.length == 0)) return;
        clear();
        for (Properties row : rows) {
            addRow(row);
        }
    }

    public void addRows(Properties [] propArray) {
        if ((propArray == null) || (propArray.length == 0)) return;
        for (Properties row : propArray) {
            addRow(row);
        }
    }

    public void replaceEntries(ArrayList<Properties> rows) {
        this.rows = rows;
    }

    public void addRow(Properties row)
    {
        if (row == null) return;
        rows.add(row);
    }

    public LinkedHashList<String,LinkedHashList> getRows()
    {
        LinkedHashList<String,LinkedHashList> rowsList = new LinkedHashList<String,LinkedHashList>(rows.size());
        for (Properties row : rows) {
            LinkedHashList<String,String> rowLHL = PropertiesUtil.prop2LinkedHashList(row);
            System.out.println(PropertiesUtil.dumpProperties("ROW", row));
            System.out.println("ROW size=" + rowLHL.size());
            rowsList.put("row", rowLHL);

        }
        return rowsList;
    }

    public void clear()
    {
        rows.clear();
    }

    public DateState getReportDate() {
        return reportDate;
    }

    public Exception getEx() {
        return ex;
    }

    public void setEx(Exception ex) {
        this.ex = ex;
    }

    public ArrayList<Properties> retrieveRows()
    {
        return rows;
    }

    public int getSize()
    {
        return rows.size();
    }
    
    public void dump(String header)
    {
        System.out.println("***" + header + "***");
        System.out.println("Date:" + reportDate.getIsoDate());
        for (int i=0; i<rows.size(); i++) {
            Properties row = getRow(i);
            System.out.println(PropertiesUtil.dumpProperties("+Row:" + i, row));
        }
    }
}
