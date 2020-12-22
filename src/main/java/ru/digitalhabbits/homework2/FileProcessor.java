package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
	private static final Logger logger = getLogger(FileProcessor.class);
	public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

	private final LineProcessor<Integer> lineProcessor = new LineCounterProcessor();

	public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
		checkFileExists(processingFileName);

		final File file = new File(processingFileName);
		var exchanger = new Exchanger<List<Pair<String, Integer>>>();
		var composeLineExecutorService = Executors.newFixedThreadPool(CHUNK_SIZE);

		var fileWriterThread = new Thread(new FileWriter(new File(resultFileName), exchanger));
		fileWriterThread.start();

		try (final Scanner scanner = new Scanner(file, defaultCharset())) {
			var futures = new ArrayList<CompletableFuture<Pair<String, Integer>>>();
			while (scanner.hasNext()) {
				for (var i = 0; i < CHUNK_SIZE; i++) {
					var line = scanner.nextLine();

					futures.add(CompletableFuture.supplyAsync(() -> lineProcessor.process(line), composeLineExecutorService));

					if (!scanner.hasNext()) {
						break;
					}
				}

				if(!futures.isEmpty()){
					exchanger.exchange(composeFutureResult(futures).get());
					futures.clear();
				}
			}
		} catch (IOException  exception) {
			logger.error("Ошибка чтения файла", exception);
		} catch (InterruptedException exception) {
			logger.error("Ошибка потока", exception);
		} catch (ExecutionException exception) {
			logger.error("Ошибка ExecutorService", exception);
		}

		fileWriterThread.interrupt();
		composeLineExecutorService.shutdown();

		logger.info("Finish main thread {}", Thread.currentThread().getName());
	}

	private void checkFileExists(@Nonnull String fileName) {
		final File file = new File(fileName);
		if (!file.exists() || file.isDirectory()) {
			throw new IllegalArgumentException("File '" + fileName + "' not exists");
		}
	}

	private CompletableFuture<List<Pair<String, Integer>>> composeFutureResult(@Nonnull ArrayList<CompletableFuture<Pair<String, Integer>>> futures) {
		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
				.thenApply(unused -> futures.stream()
						.map(CompletableFuture::join)
						.collect(Collectors.toList())
				);
	}
}
