package ru.yandex.practicum.serialization;

import org.apache.avro.specific.SpecificRecordBase;

public class SpecificRecordBaseDeserializer extends GeneralAvroDeserializer<SpecificRecordBase> {

    public SpecificRecordBaseDeserializer() {
        super(SpecificRecordBase.class);
    }
}
