package com.stockalert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import com.stockalert.alert.Alert;
import com.stockalert.alert.AlertIndexRebuilder;
import com.stockalert.alert.AlertRepository;
import com.stockalert.alert.AlertStatus;
import com.stockalert.config.KafkaConfig;
import com.stockalert.market.PriceStore;
import com.stockalert.market.PriceTick;
import com.stockalert.market.PriceTickPublisher;
import com.stockalert.notification.NotificationRepository;

import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = "app.feed.source=none")
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class AlertFlowIntegrationTests {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

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

	@Autowired
	private MockMvc mvc;

	@Autowired
	private JsonMapper json;

	@Autowired
	private PriceTickPublisher ticks;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private AlertRepository alerts;

	@Autowired
	private NotificationRepository notifications;

	@Autowired
	private AlertIndexRebuilder rebuilder;

	@Autowired
	private PriceStore priceStore;

	@Autowired
	private StringRedisTemplate redisTemplate;

	@BeforeEach
	void clearRedisIndex() {
		var keys = this.redisTemplate.keys("*");
		if (keys != null && !keys.isEmpty()) {
			this.redisTemplate.delete(keys);
		}
	}

	@Test
	void crossingATargetNotifiesTheOwnerExactlyOnceDespiteDuplicateTicks() throws Exception {
		String token = register();
		String alertId = createAlert(token, "RELIANCE", "ABOVE", "1500.00");

		publish("RELIANCE", 149995);
		publish("RELIANCE", 150000);
		publish("RELIANCE", 150000);
		publish("RELIANCE", 151000);

		awaitStatus(alertId, AlertStatus.TRIGGERED);
		await().atMost(TIMEOUT).until(() -> notificationCount(token) == 1);
		TimeUnit.SECONDS.sleep(2);

		assertThat(notificationCount(token)).isEqualTo(1);
		Alert triggered = this.alerts.findById(alertId).orElseThrow();
		assertThat(triggered.getTriggeredPaise()).isEqualTo(150000);
		await().atMost(TIMEOUT).until(() -> this.alerts.findById(alertId).orElseThrow().isEventPublished());
	}

	@Test
	void belowAlertFiresOnlyAtOrUnderTarget() throws Exception {
		String token = register();
		String alertId = createAlert(token, "SBIN", "BELOW", "750.50");

		publish("SBIN", 75055);
		await().atMost(TIMEOUT).until(() -> this.priceStore.latest("SBIN").isPresent());
		assertThat(this.alerts.findById(alertId).orElseThrow().getStatus()).isEqualTo(AlertStatus.ACTIVE);

		publish("SBIN", 75050);
		awaitStatus(alertId, AlertStatus.TRIGGERED);
	}

	@Test
	void redeliveringAnEventDoesNotCreateASecondNotification() throws Exception {
		String token = register();
		String alertId = createAlert(token, "TCS", "ABOVE", "3600.00");
		publish("TCS", 360000);
		await().atMost(TIMEOUT).until(() -> notificationCount(token) == 1);

		Alert alert = this.alerts.findById(alertId).orElseThrow();
		String event = this.json.writeValueAsString(Map.of("alertId", alertId, "userId", alert.getUserId(), "symbol",
				"TCS", "direction", "ABOVE", "targetPaise", 360000, "triggeredPaise", 360000, "triggeredAt",
				Instant.now().toString()));
		this.kafkaTemplate.send(KafkaConfig.ALERT_TRIGGERED, alert.getUserId(), event).get();
		TimeUnit.SECONDS.sleep(3);

		assertThat(notificationCount(token)).isEqualTo(1);
	}

	@Test
	void malformedMessageGoesToDeadLetterAndDoesNotBlockLaterTicks() throws Exception {
		String token = register();
		String alertId = createAlert(token, "INFY", "ABOVE", "1600.00");

		String poison = "not json " + UUID.randomUUID();
		this.kafkaTemplate.send(KafkaConfig.PRICE_TICKS, "INFY", poison).get();
		publish("INFY", 160000);

		awaitStatus(alertId, AlertStatus.TRIGGERED);
		assertThat(deadLetterValues(KafkaConfig.PRICE_TICKS + ".DLT", poison)).contains(poison);
	}

	@Test
	void ticksFromAnotherFeedAreIgnored() throws Exception {
		String token = register();
		String alertId = createAlert(token, "ICICIBANK", "ABOVE", "1400.00");

		String oldFeedTick = this.json
			.writeValueAsString(new PriceTick("ICICIBANK", 150000, Instant.now(), "simulated"));
		this.kafkaTemplate.send(KafkaConfig.PRICE_TICKS, "ICICIBANK", oldFeedTick).get();
		publish("ICICIBANK", 140500);

		awaitStatus(alertId, AlertStatus.TRIGGERED);
		assertThat(this.alerts.findById(alertId).orElseThrow().getTriggeredPaise()).isEqualTo(140500);
		assertThat(this.priceStore.latest("ICICIBANK").orElseThrow().paise()).isEqualTo(140500);
	}

	@Test
	void notificationCarriesSymbolAndTriggeredPrice() throws Exception {
		String token = register();
		createAlert(token, "RELIANCE", "BELOW", "1200.00");
		publish("RELIANCE", 119950);
		await().atMost(TIMEOUT).until(() -> notificationCount(token) == 1);

		this.mvc.perform(get("/api/notifications").header("Authorization", "Bearer " + token))
			.andExpect(jsonPath("$.content[0].symbol").value("RELIANCE"))
			.andExpect(jsonPath("$.content[0].triggeredPrice").value(1199.50));
	}

	@Test
	void listsAlertsFilteredByStatus() throws Exception {
		String token = register();
		String active = createAlert(token, "TCS", "ABOVE", "9000.00");
		String cancelled = createAlert(token, "TCS", "ABOVE", "9100.00");
		this.mvc.perform(delete("/api/alerts/" + cancelled).header("Authorization", "Bearer " + token))
			.andExpect(status().isNoContent());

		this.mvc.perform(get("/api/alerts").param("status", "ACTIVE").header("Authorization", "Bearer " + token))
			.andExpect(jsonPath("$.page.totalElements").value(1))
			.andExpect(jsonPath("$.content[0].id").value(active));
		this.mvc
			.perform(get("/api/alerts").param("status", "TRIGGERED", "CANCELLED")
				.header("Authorization", "Bearer " + token))
			.andExpect(jsonPath("$.page.totalElements").value(1))
			.andExpect(jsonPath("$.content[0].id").value(cancelled));
		this.mvc.perform(get("/api/alerts").header("Authorization", "Bearer " + token))
			.andExpect(jsonPath("$.page.totalElements").value(2))
			.andExpect(jsonPath("$.page.totalPages").value(1))
			.andExpect(jsonPath("$.page.number").value(0));
		this.mvc.perform(get("/api/alerts").param("status", "NONSENSE").header("Authorization", "Bearer " + token))
			.andExpect(status().isBadRequest());
	}

	@Test
	void instrumentsIncludeIsin() throws Exception {
		String token = register();
		this.mvc.perform(get("/api/market/instruments").header("Authorization", "Bearer " + token))
			.andExpect(jsonPath("$[0].symbol").value("RELIANCE"))
			.andExpect(jsonPath("$[0].isin").value("INE002A01018"));
	}

	@Test
	void rejectsOutOfRangePaging() throws Exception {
		String token = register();
		this.mvc.perform(get("/api/alerts").param("size", "0").header("Authorization", "Bearer " + token))
			.andExpect(status().isBadRequest());
		this.mvc.perform(get("/api/notifications").param("size", "101").header("Authorization", "Bearer " + token))
			.andExpect(status().isBadRequest());
	}

	@Test
	void rejectsPasswordTooLongForBcrypt() throws Exception {
		String body = this.json.writeValueAsString(Map.of("email", uniqueEmail(), "password", "₹".repeat(30)));
		this.mvc.perform(post("/api/auth/register").contentType(MediaType.APPLICATION_JSON).content(body))
			.andExpect(status().isBadRequest());
	}

	private List<String> deadLetterValues(String topic, String expected) {
		Map<String, Object> config = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
				ConsumerConfig.GROUP_ID_CONFIG, "dlt-check-" + UUID.randomUUID(),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		List<String> values = new ArrayList<>();
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(),
				new StringDeserializer())) {
			consumer.subscribe(List.of(topic));
			await().atMost(TIMEOUT).until(() -> {
				consumer.poll(Duration.ofMillis(500)).forEach((record) -> values.add(record.value()));
				return values.contains(expected);
			});
		}
		return values;
	}

	@Test
	void cancelledAlertNeverFires() throws Exception {
		String token = register();
		String alertId = createAlert(token, "ITC", "ABOVE", "500.00");

		this.mvc.perform(delete("/api/alerts/" + alertId).header("Authorization", "Bearer " + token))
			.andExpect(status().isNoContent());
		publish("ITC", 50000);
		await().atMost(TIMEOUT).until(() -> this.priceStore.latest("ITC").isPresent());
		TimeUnit.SECONDS.sleep(2);

		assertThat(this.alerts.findById(alertId).orElseThrow().getStatus()).isEqualTo(AlertStatus.CANCELLED);
		assertThat(notificationCount(token)).isZero();
	}

	@Test
	void indexIsRebuiltFromMongoAfterRedisLosesData() throws Exception {
		String token = register();
		String alertId = createAlert(token, "LT", "ABOVE", "3700.00");
		clearRedisIndex();

		this.rebuilder.rebuild();
		publish("LT", 370000);

		awaitStatus(alertId, AlertStatus.TRIGGERED);
	}

	@Test
	void olderTickDoesNotOverwriteNewerPrice() {
		Instant now = Instant.now();
		assertThat(this.priceStore.updateIfNewer(new PriceTick("BHARTIARTL", 200000, now))).isTrue();
		assertThat(this.priceStore.updateIfNewer(new PriceTick("BHARTIARTL", 190000, now.minusSeconds(5)))).isFalse();
		assertThat(this.priceStore.latest("BHARTIARTL").orElseThrow().paise()).isEqualTo(200000);
	}

	@Test
	void tradesWithTheSameTimestampBothUpdateThePrice() {
		Instant now = Instant.now();
		this.priceStore.updateIfNewer(new PriceTick("BHARTIARTL", 200000, now));
		assertThat(this.priceStore.updateIfNewer(new PriceTick("BHARTIARTL", 200010, now))).isTrue();
		assertThat(this.priceStore.latest("BHARTIARTL").orElseThrow().paise()).isEqualTo(200010);
	}

	@Test
	void usersCannotSeeOrCancelEachOthersAlerts() throws Exception {
		String owner = register();
		String other = register();
		String alertId = createAlert(owner, "HDFCBANK", "ABOVE", "1800.00");

		this.mvc.perform(delete("/api/alerts/" + alertId).header("Authorization", "Bearer " + other))
			.andExpect(status().isNotFound());
		this.mvc.perform(get("/api/alerts").header("Authorization", "Bearer " + other))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.content.length()").value(0));
	}

	@Test
	void rejectsRequestsWithoutToken() throws Exception {
		this.mvc.perform(get("/api/alerts")).andExpect(status().isUnauthorized());
	}

	@Test
	void validatesAlertRequests() throws Exception {
		String token = register();
		postAlert(token, "UNKNOWNCO", "ABOVE", "100.00").andExpect(status().isNotFound());
		postAlert(token, "ICICIBANK", "ABOVE", "-5").andExpect(status().isBadRequest());
		postAlert(token, "ICICIBANK", "ABOVE", "100.123").andExpect(status().isBadRequest());
	}

	@Test
	void rejectsTargetThePriceHasAlreadyCrossed() throws Exception {
		String token = register();
		this.priceStore.updateIfNewer(new PriceTick("HINDUNILVR", 250000, Instant.now()));

		postAlert(token, "HINDUNILVR", "ABOVE", "2400.00").andExpect(status().isUnprocessableContent());
		postAlert(token, "HINDUNILVR", "BELOW", "2400.00").andExpect(status().isCreated());
	}

	@Test
	void loginRejectsWrongPasswordAndAcceptsCorrectOne() throws Exception {
		String email = uniqueEmail();
		String body = this.json.writeValueAsString(Map.of("email", email, "password", "test-password-1"));
		this.mvc.perform(post("/api/auth/register").contentType(MediaType.APPLICATION_JSON).content(body))
			.andExpect(status().isCreated());
		this.mvc.perform(post("/api/auth/register").contentType(MediaType.APPLICATION_JSON).content(body))
			.andExpect(status().isConflict());
		this.mvc.perform(post("/api/auth/login").contentType(MediaType.APPLICATION_JSON).content(body))
			.andExpect(status().isOk())
			.andExpect(jsonPath("$.token").isNotEmpty());
		String wrong = this.json.writeValueAsString(Map.of("email", email, "password", "wrong-password"));
		this.mvc.perform(post("/api/auth/login").contentType(MediaType.APPLICATION_JSON).content(wrong))
			.andExpect(status().isUnauthorized());
	}

	private void publish(String symbol, long paise) {
		this.ticks.publish(new PriceTick(symbol, paise, Instant.now()));
	}

	private void awaitStatus(String alertId, AlertStatus expected) {
		await().atMost(TIMEOUT).until(() -> this.alerts.findById(alertId).orElseThrow().getStatus() == expected);
	}

	private String register() throws Exception {
		String body = this.json.writeValueAsString(Map.of("email", uniqueEmail(), "password", "test-password-1"));
		String response = this.mvc.perform(post("/api/auth/register").contentType(MediaType.APPLICATION_JSON).content(body))
			.andExpect(status().isCreated())
			.andReturn()
			.getResponse()
			.getContentAsString();
		return this.json.readTree(response).get("token").asString();
	}

	private static String uniqueEmail() {
		return "user-" + UUID.randomUUID() + "@example.test";
	}

	private String createAlert(String token, String symbol, String direction, String target) throws Exception {
		String response = postAlert(token, symbol, direction, target).andExpect(status().isCreated())
			.andReturn()
			.getResponse()
			.getContentAsString();
		return this.json.readTree(response).get("id").asString();
	}

	private org.springframework.test.web.servlet.ResultActions postAlert(String token, String symbol, String direction,
			String target) throws Exception {
		String body = "{\"symbol\":\"%s\",\"direction\":\"%s\",\"targetPrice\":%s}".formatted(symbol, direction,
				target);
		return this.mvc.perform(post("/api/alerts").header("Authorization", "Bearer " + token)
			.contentType(MediaType.APPLICATION_JSON)
			.content(body));
	}

	private int notificationCount(String token) throws Exception {
		String response = this.mvc.perform(get("/api/notifications").header("Authorization", "Bearer " + token))
			.andExpect(status().isOk())
			.andReturn()
			.getResponse()
			.getContentAsString();
		return this.json.readTree(response).get("content").size();
	}

}
