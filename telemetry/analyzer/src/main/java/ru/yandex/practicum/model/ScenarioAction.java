package ru.yandex.practicum.model;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@Entity
@Table(name = "scenario_actions")
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ScenarioAction {
    @EmbeddedId
    private ScenarioActionCompositeKey key;

    @ManyToOne
    @JoinColumn(name = "scenario_id")
    Scenario scenario;

    @ManyToOne
    @JoinColumn(name = "sensor_id")
    Sensor sensor;

    @ManyToOne
    @JoinColumn(name = "action_id")
    Action action;
}
