import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import javax.swing.text.Document;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CrptApi {
    private final Semaphore semaphore;
    private final long resetTime;
    private long requestLimit;
    private final OkHttpClient okHttpClient;
    private final ObjectMapper objectMapper;

    public CrptApi(TimeUnit timeUnit, int requestLimit, OkHttpClient okHttpClient, ObjectMapper objectMapper) {
        this.semaphore = new Semaphore(requestLimit);
        this.resetTime = timeUnit.toMillis(1);
        this.requestLimit = requestLimit;
        this.okHttpClient = okHttpClient;
        this.objectMapper = objectMapper;
    }

    public Flowable<Response> createDocument(Document document) {
        return Flowable.defer(() -> {
                    // Получаем текущее время
                    Instant now = Instant.now();
                    // Ждем, пока не истечет текущий интервал времени
                    while (now.plusMillis(resetTime).isAfter(Instant.now())) {
                        Thread.sleep(100);
                    }
                    // Попробуем получить разрешение на выполнение запроса
                    if (semaphore.tryAcquire()) {
                        try {
                            // Объект Java в JSON формат
                            String json = objectMapper.writeValueAsString(document);
                            // Создать запрос
                            Request request = new Request.Builder()
                                    .url("https://ismp.crpt.ru/api/v3/lk/documents/create")
                                    .post(RequestBody.create(json, null))
                                    .build();
                            // Отправляем запрос асинхронно
                            return Flowable.fromCallable(() -> okHttpClient.newCall(request).execute())
                                    .subscribeOn(Schedulers.io());
                        } finally {
                            // Освободить разрешение на выполнение запроса
                            semaphore.release();
                        }
                    } else {
                        // Превышен лимит количества запросов
                        // Подождать, пока не истечет текущий интервал времени
                        return Flowable.timer(resetTime, TimeUnit.MILLISECONDS)
                                .flatMap(__ -> createDocument(document));
                    }
                })
                .retryWhen(errors -> errors.flatMap(error -> {
                    if (error instanceof IOException) {
                        // Превышен лимит количества запросов
                        // Подождать, пока не истечет текущий интервал времени
                        return Flowable.timer(resetTime, TimeUnit.MILLISECONDS);
                    }
                    return Flowable.error(error);
                }))
                .doOnNext(response -> {
                    // Обработываем ответ
                    // Уменьшаем requestLimit
                    requestLimit--;
                    if (requestLimit == 0) {
                        System.out.println("Программа завершена: достигнут лимит запросов.");
                        System.exit(0);
                    }
                });
    }
}