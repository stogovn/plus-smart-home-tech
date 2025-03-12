package ru.yandex.practicum.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Entity
@Table(name = "sensors")
public class Sensor {

    @Id
    @Column(nullable = false)
    private String id;

    @Column(name = "hub_id", nullable = false)
    private String hubId;
}
