package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
		implements Runnable {
	private static final Logger logger = getLogger(FileWriter.class);

	private final File file;
	private final Exchanger<List<Pair<String, Integer>>> exchanger;

	public FileWriter(File file, Exchanger<List<Pair<String, Integer>>> exchanger) {
		this.file = file;
		this.exchanger = exchanger;
	}

	@Override
	public void run() {
		logger.info("Started writer thread {}", currentThread().getName());

		try (java.io.FileWriter fileWriter = new java.io.FileWriter(file, false)) {
			while (!currentThread().isInterrupted()) {
				var writeData = exchanger.exchange(null);
				if (writeData.isEmpty()) {
					continue;
				}

				for (var lineData : writeData) {
					fileWriter.write(String.format("%s %d/n", lineData.getKey(), lineData.getValue()));
				}
			}
		} catch (IOException exception) {
			logger.error("Ошибка при попытке открыть файл", exception);
		} catch (InterruptedException exception) {
			logger.error("Не удалось получить данные от других потов", exception);
		}

		logger.info("Finish writer thread {}", currentThread().getName());
	}
}
