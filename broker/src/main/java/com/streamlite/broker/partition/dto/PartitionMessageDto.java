package com.streamlite.broker.partition.dto;

import lombok.*;

import java.time.OffsetDateTime;
import java.util.Map;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitionMessageDto {

    private OffsetDateTime timestamp;
    private String msgKey;
    private String msgValue;
    private String msgHeaders;
    private Integer priority;
    private Long offset;

}
