package ru.yandex.practicum.service;

public interface HubService<T> {

    void process(T event);
}
