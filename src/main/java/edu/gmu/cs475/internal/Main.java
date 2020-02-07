package edu.gmu.cs475.internal;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.shell.jline.PromptProvider;

@SpringBootApplication
public class Main {
	public static int port;

	public static void main(String[] args) {
		SpringApplication.run(Main.class, args);
		Command.cleanup();
	}

	@Bean
	public PromptProvider promptProvider() {
		return new PromptProvider() {

			@Override
			public AttributedString getPrompt() {
				return new AttributedString("cs475sh:>", AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW));
			}
		};
	}
}
