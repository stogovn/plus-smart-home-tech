package ru.yandex.practicum.repository.persister;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.model.Sensor;
import ru.yandex.practicum.repository.SensorRepository;

import java.util.List;

@Component
@Transactional
@RequiredArgsConstructor
public class SensorPersister {

    private final SensorRepository repository;

    public Sensor save(Sensor sensor) {
        return repository.save(sensor);
    }

    public void removeById(String id) {
        repository.deleteById(id);
    }

    @Transactional(readOnly = true)
    public List<Sensor> getSensorsByIds(List<String> ids) {
        return repository.findAllById(ids);
    }

    @Transactional(readOnly = true)
    public List<Sensor> getSensorsByHubId(String id) {
        return repository.findAllByHubId(id).getContent();
    }
}
