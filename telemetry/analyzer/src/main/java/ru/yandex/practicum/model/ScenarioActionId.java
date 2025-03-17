package ru.yandex.practicum.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.Setter;
import org.antlr.v4.runtime.misc.NotNull;
import org.hibernate.Hibernate;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

@Getter
@Setter
@Embeddable
public class ScenarioActionId implements Serializable {

    @Serial
    private static final long serialVersionUID = -7825093976943355832L;
    @NotNull
    @Column(name = "scenario_id", nullable = false)
    private Long scenarioId;

    @NotNull
    @Column(name = "sensor_id", nullable = false, length = Integer.MAX_VALUE)
    private String sensorId;

    @NotNull
    @Column(name = "action_id", nullable = false)
    private Long actionId;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || Hibernate.getClass(this) != Hibernate.getClass(o)) {
            return false;
        }
        ScenarioActionId entity = (ScenarioActionId) o;
        return Objects.equals(this.actionId, entity.actionId) &&
               Objects.equals(this.scenarioId, entity.scenarioId) &&
               Objects.equals(this.sensorId, entity.sensorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionId, scenarioId, sensorId);
    }

}
