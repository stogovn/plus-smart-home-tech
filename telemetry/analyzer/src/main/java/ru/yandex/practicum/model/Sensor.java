package ru.yandex.practicum.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashSet;
import java.util.Set;

@Entity
@Getter
@Setter
@Table(name = "sensors")
public class Sensor {

    @Id
    @Column(name = "id", nullable = false, length = Integer.MAX_VALUE)
    private String id;

    @Column(name = "hub_id", length = Integer.MAX_VALUE)
    private String hubId;

    @OneToMany(mappedBy = "sensor")
    private Set<ScenarioAction> scenarioActions = new LinkedHashSet<>();

    @OneToMany(mappedBy = "sensor")
    private Set<ScenarioCondition> scenarioConditions = new LinkedHashSet<>();

}
