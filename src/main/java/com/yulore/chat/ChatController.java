package com.yulore.chat;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
@RequestMapping("/api/chat")
@Slf4j
public class ChatController {

    private final BaiLianService baiLianService;
    private final GScriptService gscriptService;

    public ChatController(BaiLianService baiLianService, GScriptService gscriptService) {
        this.baiLianService = baiLianService;
        this.gscriptService = gscriptService;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<Flux<String>> stream(@RequestParam String model, @RequestParam String prompt) {
        log.info("stream: {}", prompt);

        return ResponseEntity.ok()
                .contentType(MediaType.valueOf("text/event-stream;charset=UTF-8"))
                .body(baiLianService.streamChat(model, prompt));
    }

    @Data
    @ToString
    public static class Item {
        private String role;
        private String content;
    }

    @PostMapping(value = "/script", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<Flux<String>> script(@RequestBody List<Item> contents) {
        log.info("script: {}", contents);

        return ResponseEntity.ok()
                .contentType(MediaType.valueOf("text/event-stream;charset=UTF-8"))
                .body(gscriptService.streamChat(contents));
    }
}