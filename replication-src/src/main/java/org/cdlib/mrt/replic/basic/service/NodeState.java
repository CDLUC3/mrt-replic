/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.replic.basic.service;

import org.cdlib.mrt.utility.StateInf;

/**
 *
 * @author loy
 */
public class NodeState 
    implements StateInf
{
    protected Long node = null;
    protected Long id = null;
    protected String status = null;
    public NodeState(Long node, Long id) {
       this.node = node;
       this.id = id;
    }

    public Long getNode() {
        return node;
    }

    public void setNode(Long node) {
        this.node = node;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    
}
