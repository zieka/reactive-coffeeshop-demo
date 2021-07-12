package me.escoffier.quarkus.coffeeshop;


import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import io.smallrye.reactive.messaging.connectors.InMemorySource;
import java.util.List;
import javax.enterprise.inject.Any;
import javax.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@QuarkusTest
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
class BaristaTest {
    
    @Inject
    @Any
    InMemoryConnector connector;

    @Test
    void testProcessOrder() {
        InMemorySource<Order> orders = connector.source("orders");
        InMemorySink<Beverage> queue = connector.sink("queue");

        Order order = new Order();
        order.setProduct("coffee");
        order.setName("Coffee lover");
        order.setOrderId("1234");

        orders.send(order);
        
        await().<List<? extends Message<Beverage>>>until(queue::received, t -> t.size() == 1);

        Beverage queuedBeverage = queue.received().get(0).getPayload();
        Assertions.assertEquals(Beverage.State.READY, queuedBeverage.preparationState);
        assertThat(queuedBeverage.beverage).isEqualTo("coffee");
        assertThat(queuedBeverage.customer).isEqualTo("Coffee lover");
        assertThat(queuedBeverage.orderId).isEqualTo("1234");
    }

}
