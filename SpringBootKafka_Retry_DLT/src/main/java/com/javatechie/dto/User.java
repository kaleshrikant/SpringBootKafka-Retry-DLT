package com.javatechie.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class User {

    private int id;
    private String firstName;
    private String lastName;
    private String email;
    private String gender;
    private String ipAddress;
}
