package ru.yandex.practicum.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.Setter;
import org.antlr.v4.runtime.misc.NotNull;
import org.hibernate.Hibernate;

import java.io.Serial;
import java.util.Objects;

@Getter
@Setter
@Embeddable
public class ScenarioConditionId implements java.io.Serializable {

    @Serial
    private static final long serialVersionUID = -493809935835829631L;
    @NotNull
    @Column(name = "scenario_id", nullable = false)
    private Long scenarioId;

    @NotNull
    @Column(name = "sensor_id", nullable = false, length = Integer.MAX_VALUE)
    private String sensorId;

    @NotNull
    @Column(name = "condition_id", nullable = false)
    private Long conditionId;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || Hibernate.getClass(this) != Hibernate.getClass(o))
            return false;
        ScenarioConditionId entity = (ScenarioConditionId) o;
        return Objects.equals(this.conditionId, entity.conditionId) &&
               Objects.equals(this.scenarioId, entity.scenarioId) &&
               Objects.equals(this.sensorId, entity.sensorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conditionId, scenarioId, sensorId);
    }

}
