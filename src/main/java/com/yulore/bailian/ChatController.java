package com.yulore.bailian;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/chat")
@Slf4j
public class ChatController {

    private final BaiLianService baiLianService;

    public ChatController(BaiLianService baiLianService) {
        this.baiLianService = baiLianService;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<Flux<String>> stream(@RequestParam String model, @RequestParam String prompt) {
        log.info("stream: {}", prompt);

        return ResponseEntity.ok()
                .contentType(MediaType.valueOf("text/event-stream;charset=UTF-8"))
                .body(baiLianService.streamChat(model, prompt));
    }
}