package com.yulore.medhub.api;


import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class ApiResponse<DATA> {
    private String code;
    private String message;
    private DATA data;
}
