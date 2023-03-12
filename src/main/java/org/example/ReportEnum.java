package org.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

public enum ReportEnum {
    MISSING, NOT_EQUAL, EQUAL
}
enum Source {
    EXISTING, NEW
}

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
class ReportRecord {
    private ReportEnum reportEnum;
    private Source source;
    private String id;

    static ReportRecord EMPTY = new ReportRecord();
}
