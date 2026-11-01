package com.stockalert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import com.stockalert.market.InstrumentCatalogLoader;
import com.stockalert.market.MarketHours;
import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

@SpringBootTest(properties = { "app.feed.source=yahoo", "app.yahoo.poll-interval=1s",
		"app.yahoo.on-demand-per-minute=1000" })
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class FullCatalogIntegrationTests {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private static final String CATALOG = """
			[
			  {"segment":"NSE_EQ","instrument_type":"EQ","trading_symbol":"WIPRO","name":"WIPRO LTD","isin":"INE075A01022"},
			  {"segment":"NSE_EQ","instrument_type":"EQ","trading_symbol":"M&M","name":"MAHINDRA & MAHINDRA LTD","isin":"INE101A01026"},
			  {"segment":"NSE_EQ","instrument_type":"SM","trading_symbol":"SMALLCO","name":"SMALL COMPANY LTD","isin":"INE000A00001"},
			  {"segment":"NSE_EQ","instrument_type":"EQ","trading_symbol":"NOPRICE","name":"NO PRICE LTD","isin":"INE000A00002"},
			  {"segment":"NSE_EQ","instrument_type":"N1","trading_symbol":"805BOND29","name":"SOME BOND","isin":"INE000A00003"},
			  {"segment":"NSE_FO","instrument_type":"EQ","trading_symbol":"WIPROFUT","name":"WIPRO FUTURE","isin":"INE000A00004"}
			]
			""";

	@Container
	@ServiceConnection
	static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

	@Container
	@ServiceConnection
	static final MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:8.2"));

	@Container
	@ServiceConnection(name = "redis")
	@SuppressWarnings("resource")
	static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
		.withExposedPorts(6379);

	private static final Map<String, Double> yahooPrices = new ConcurrentHashMap<>();

	private static final List<String> yahooRequests = new CopyOnWriteArrayList<>();

	private static final List<String> historyRequests = new CopyOnWriteArrayList<>();

	private static final List<String> fundamentalsRequests = new CopyOnWriteArrayList<>();

	private static final String FUNDAMENTALS = """
			{"timeseries":{"result":[
			  {"meta":{"type":["annualNetIncome"]},"annualNetIncome":[
			    null,
			    {"asOfDate":"2024-03-31","reportedValue":{"raw":80}},
			    {"asOfDate":"2026-03-31","reportedValue":{"raw":100}}]},
			  {"meta":{"type":["annualTotalRevenue"]},"annualTotalRevenue":[
			    {"asOfDate":"2024-03-31","reportedValue":{"raw":800}},
			    {"asOfDate":"2026-03-31","reportedValue":{"raw":1000}}]},
			  {"meta":{"type":["annualStockholdersEquity"]},"annualStockholdersEquity":[
			    {"asOfDate":"2026-03-31","reportedValue":{"raw":500}}]},
			  {"meta":{"type":["annualTotalDebt"]},"annualTotalDebt":[
			    {"asOfDate":"2026-03-31","reportedValue":{"raw":250}}]},
			  {"meta":{"type":["annualOperatingCashFlow"]},"annualOperatingCashFlow":[
			    {"asOfDate":"2026-03-31","reportedValue":{"raw":120}}]},
			  {"meta":{"type":["trailingPbRatio"]},"trailingPbRatio":[
			    {"asOfDate":"2026-10-30","reportedValue":{"raw":2.5}}]},
			  {"meta":{"type":["trailingPeRatio"]}}
			]}}
			""";

	private static volatile boolean yahooHistoryFails;

	private static final HttpServer fakeServer = startFakeServer();

	@DynamicPropertySource
	static void fakeServerProperties(DynamicPropertyRegistry registry) {
		String base = "http://localhost:" + fakeServer.getAddress().getPort();
		registry.add("app.yahoo.base-url", () -> base);
		registry.add("app.catalog.url", () -> base + "/catalog.json.gz");
	}

	@AfterAll
	static void stopFakeServer() {
		fakeServer.stop(0);
	}

	@MockitoBean
	private MarketHours marketHours;

	@Autowired
	private MockMvc mvc;

	@Autowired
	private JsonMapper json;

	@Autowired
	private InstrumentCatalogLoader loader;

	@Autowired
	private StringRedisTemplate redisTemplate;

	@BeforeEach
	void marketIsOpen() {
		given(this.marketHours.isOpen(any(Instant.class))).willReturn(true);
		this.loader.load();
	}

	@Test
	void searchFindsEquitiesBySymbolNameOrIsinAndLeavesOutOtherInstruments() throws Exception {
		String token = register();

		search(token, "wip").andExpect(jsonPath("$[0].symbol").value("WIPRO"))
			.andExpect(jsonPath("$[0].isin").value("INE075A01022"))
			.andExpect(jsonPath("$.length()").value(1));
		search(token, "mahindra").andExpect(jsonPath("$[0].symbol").value("M&M"));
		search(token, "INE101A").andExpect(jsonPath("$[0].symbol").value("M&M"));
		search(token, "smallco").andExpect(jsonPath("$[0].symbol").value("SMALLCO"));
		search(token, "805BOND").andExpect(jsonPath("$.length()").value(0));
		search(token, "   ").andExpect(jsonPath("$.length()").value(0));
	}

	@Test
	void searchTreatsRegexCharactersAsPlainText() throws Exception {
		String token = register();

		search(token, ".*").andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(0));
		search(token, "(").andExpect(status().isOk()).andExpect(jsonPath("$.length()").value(0));
	}

	@Test
	void aStockOutsideTheDefaultsGetsAPriceWhenRequested() throws Exception {
		String token = register();
		yahooPrices.put("M&M.NS", 2840.30);

		this.mvc.perform(get("/api/market/prices/{symbol}", "m&m").header("Authorization", "Bearer " + token))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.symbol").value("M&M"))
			.andExpect(jsonPath("$.price").value(2840.30))
			.andExpect(jsonPath("$.source").value("yahoo"));
	}

	@Test
	void unknownSymbolsAreRejected() throws Exception {
		String token = register();

		this.mvc.perform(get("/api/market/prices/DOESNOTEXIST").header("Authorization", "Bearer " + token))
			.andExpect(status().isNotFound());
	}

	@Test
	void anAlertCannotBeSetOnAStockWithNoPrice() throws Exception {
		String token = register();

		postAlert(token, "NOPRICE", "ABOVE", "100.00").andExpect(status().isConflict());
	}

	@Test
	void anAlertOnAnyStockKeepsItPolledAndFires() throws Exception {
		String token = register();
		yahooPrices.put("WIPRO.NS", 198.00);
		String alert = postAlert(token, "WIPRO", "ABOVE", "200.00").andExpect(status().isCreated())
			.andReturn()
			.getResponse()
			.getContentAsString();
		String alertId = this.json.readTree(alert).get("id").asString();

		yahooRequests.clear();
		await().atMost(TIMEOUT).until(() -> yahooRequests.contains("WIPRO.NS"));

		yahooPrices.put("WIPRO.NS", 200.50);

		await().atMost(TIMEOUT).until(() -> notificationCount(token) == 1);
		this.mvc.perform(get("/api/notifications").header("Authorization", "Bearer " + token))
			.andExpect(jsonPath("$.content[0].alertId").value(alertId))
			.andExpect(jsonPath("$.content[0].symbol").value("WIPRO"))
			.andExpect(jsonPath("$.content[0].triggeredPrice").value(200.50));

		// Once triggered, the stock has no active alert and should stop being polled.
		Thread.sleep(2500);
		yahooRequests.clear();
		Thread.sleep(2500);
		assertThat(yahooRequests).doesNotContain("WIPRO.NS");
	}

	@Test
	void historyShowsTheRequestedRangeWithIndicatorsWarmedUpFromEarlierData() throws Exception {
		String token = register();

		String body = history(token, "SMALLCO", "1M").andExpect(status().isOk())
			.andExpect(jsonPath("$.symbol").value("SMALLCO"))
			.andExpect(jsonPath("$.range").value("1M"))
			.andExpect(jsonPath("$.source").value("yahoo"))
			.andExpect(jsonPath("$.stale").value(false))
			.andExpect(jsonPath("$.fiftyTwoWeekHigh").value(240.5))
			.andExpect(jsonPath("$.points[0].ma20").isNumber())
			.andExpect(jsonPath("$.points[0].ma50").isNumber())
			.andExpect(jsonPath("$.points[0].rsi14").isNumber())
			.andReturn()
			.getResponse()
			.getContentAsString();

		// 100 daily closes were fetched, but only the last 30 days should be returned.
		int points = this.json.readTree(body).get("points").size();
		assertThat(points).isBetween(30, 31);
		assertThat(this.json.readTree(body).get("points").get(points - 1).get("close").asDouble()).isEqualTo(199.0);
	}

	@Test
	void historySkipsGapsAndIsCachedSoYahooIsAskedOnce() throws Exception {
		String token = register();
		historyRequests.clear();

		history(token, "M&M", "6M").andExpect(status().isOk());
		history(token, "M&M", "6M").andExpect(status().isOk());

		assertThat(historyRequests).hasSize(1);
		// The fake's point 50 has a null close, so nothing in the response can be null.
		String body = history(token, "M&M", "6M").andReturn().getResponse().getContentAsString();
		assertThat(body).doesNotContain("\"close\":null");
	}

	@Test
	void historyFallsBackToTheSavedCopyWhenYahooFails() throws Exception {
		String token = register();
		history(token, "WIPRO", "1Y").andExpect(status().isOk());
		// Make the saved copy old enough to need refreshing.
		String key = "history:WIPRO:1Y";
		ObjectNode saved = (ObjectNode) this.json.readTree(this.redisTemplate.opsForValue().get(key));
		saved.put("fetchedAt", "2020-01-01T00:00:00Z");
		this.redisTemplate.opsForValue().set(key, this.json.writeValueAsString(saved));

		yahooHistoryFails = true;
		try {
			history(token, "WIPRO", "1Y").andExpect(status().isOk()).andExpect(jsonPath("$.stale").value(true));
		}
		finally {
			yahooHistoryFails = false;
			// The failure correctly made the feed back off for one poll interval, so wait that out.
			Thread.sleep(1500);
		}
	}

	@Test
	void historyRejectsUnknownRangesAndStocks() throws Exception {
		String token = register();

		history(token, "WIPRO", "99Y").andExpect(status().isBadRequest());
		history(token, "DOESNOTEXIST", "1M").andExpect(status().isNotFound());
		this.mvc.perform(get("/api/market/history/WIPRO")).andExpect(status().isUnauthorized());
	}

	@Test
	void scorecardCombinesCompanyResultsWithPricesAndIsCached() throws Exception {
		String token = register();

		this.mvc.perform(get("/api/market/scorecard/SMALLCO").header("Authorization", "Bearer " + token))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.symbol").value("SMALLCO"))
			.andExpect(jsonPath("$.score").isNumber())
			.andExpect(jsonPath("$.band").isString())
			.andExpect(jsonPath("$.confidence").isString())
			.andExpect(jsonPath("$.financialCompany").value(false))
			.andExpect(jsonPath("$.financialsAsOf").value("2026-03-31"))
			.andExpect(jsonPath("$.categories[0].name").value("Profitability"))
			.andExpect(jsonPath("$.categories[0].parameters[0].name").value("Return on equity"))
			.andExpect(jsonPath("$.categories[0].parameters[0].value").value("20.0%"))
			.andExpect(jsonPath("$.stale").value(false));

		int requests = fundamentalsRequests.size();
		this.mvc.perform(get("/api/market/scorecard/SMALLCO").header("Authorization", "Bearer " + token))
			.andExpect(status().isOk());
		assertThat(fundamentalsRequests).hasSize(requests);
	}

	@Test
	void scorecardRejectsUnknownStocksAndNeedsSignIn() throws Exception {
		String token = register();

		this.mvc.perform(get("/api/market/scorecard/DOESNOTEXIST").header("Authorization", "Bearer " + token))
			.andExpect(status().isNotFound());
		this.mvc.perform(get("/api/market/scorecard/WIPRO")).andExpect(status().isUnauthorized());
	}

	private org.springframework.test.web.servlet.ResultActions history(String token, String symbol, String range)
			throws Exception {
		return this.mvc.perform(get("/api/market/history/{symbol}", symbol).param("range", range)
			.header("Authorization", "Bearer " + token));
	}

	private static String historyJson() {
		long now = Instant.now().getEpochSecond();
		StringBuilder times = new StringBuilder();
		StringBuilder closes = new StringBuilder();
		StringBuilder volumes = new StringBuilder();
		for (int i = 0; i < 100; i++) {
			String separator = (i == 0) ? "" : ",";
			times.append(separator).append(now - (99 - i) * 86400L);
			closes.append(separator).append((i == 50) ? "null" : String.valueOf(100.0 + i));
			volumes.append(separator).append(1000 + i);
		}
		return """
				{"chart":{"result":[{"meta":{"currency":"INR","fiftyTwoWeekHigh":240.5,"fiftyTwoWeekLow":90.25},
				"timestamp":[%s],"indicators":{"quote":[{"open":[%s],"high":[%s],"low":[%s],"close":[%s],"volume":[%s]}]}}]}}
				""".formatted(times, closes, closes, closes, closes, volumes);
	}

	private org.springframework.test.web.servlet.ResultActions search(String token, String term) throws Exception {
		return this.mvc.perform(get("/api/market/instruments/search").param("q", term)
			.header("Authorization", "Bearer " + token));
	}

	private org.springframework.test.web.servlet.ResultActions postAlert(String token, String symbol, String direction,
			String target) throws Exception {
		String body = "{\"symbol\":\"%s\",\"direction\":\"%s\",\"targetPrice\":%s}".formatted(symbol, direction,
				target);
		return this.mvc.perform(post("/api/alerts").header("Authorization", "Bearer " + token)
			.contentType(MediaType.APPLICATION_JSON)
			.content(body));
	}

	private String register() throws Exception {
		String body = this.json.writeValueAsString(
				Map.of("email", "user-" + UUID.randomUUID() + "@example.test", "password", "test-password-1"));
		String response = this.mvc
			.perform(post("/api/auth/register").contentType(MediaType.APPLICATION_JSON).content(body))
			.andExpect(status().isCreated())
			.andReturn()
			.getResponse()
			.getContentAsString();
		return this.json.readTree(response).get("token").asString();
	}

	private int notificationCount(String token) throws Exception {
		String response = this.mvc.perform(get("/api/notifications").header("Authorization", "Bearer " + token))
			.andExpect(status().isOk())
			.andReturn()
			.getResponse()
			.getContentAsString();
		return this.json.readTree(response).get("content").size();
	}

	private static HttpServer startFakeServer() {
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
			server.createContext("/catalog.json.gz", (exchange) -> {
				ByteArrayOutputStream bytes = new ByteArrayOutputStream();
				try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
					gzip.write(CATALOG.getBytes(StandardCharsets.UTF_8));
				}
				exchange.sendResponseHeaders(200, bytes.size());
				exchange.getResponseBody().write(bytes.toByteArray());
				exchange.close();
			});
			server.createContext("/ws/fundamentals-timeseries/", (exchange) -> {
				fundamentalsRequests.add(exchange.getRequestURI().getPath());
				byte[] body = FUNDAMENTALS.getBytes(StandardCharsets.UTF_8);
				exchange.sendResponseHeaders(200, body.length);
				exchange.getResponseBody().write(body);
				exchange.close();
			});
			server.createContext("/v8/finance/chart/", (exchange) -> {
				String path = exchange.getRequestURI().getPath();
				String ticker = path.substring(path.lastIndexOf('/') + 1);
				// History requests lead with the range; price requests lead with the interval.
				if (exchange.getRequestURI().getQuery().startsWith("range=")) {
					historyRequests.add(ticker);
					byte[] history = historyJson().getBytes(StandardCharsets.UTF_8);
					exchange.sendResponseHeaders(yahooHistoryFails ? 500 : 200, history.length);
					exchange.getResponseBody().write(history);
					exchange.close();
					return;
				}
				yahooRequests.add(ticker);
				Double price = yahooPrices.get(ticker);
				byte[] body = ((price == null) ? "{}"
						: "{\"chart\":{\"result\":[{\"meta\":{\"regularMarketPrice\":%s,\"regularMarketTime\":%d}}]}}"
							.formatted(price, Instant.now().getEpochSecond()))
					.getBytes(StandardCharsets.UTF_8);
				exchange.sendResponseHeaders((price == null) ? 404 : 200, body.length);
				exchange.getResponseBody().write(body);
				exchange.close();
			});
			server.start();
			return server;
		}
		catch (IOException ex) {
			throw new IllegalStateException(ex);
		}
	}

}
