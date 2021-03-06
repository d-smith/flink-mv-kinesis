package com.myorg;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.kinesis.Stream;

public class InfrastructureStack extends Stack {
    public InfrastructureStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public InfrastructureStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);


        Stream.Builder.create(this, "quotestream")
            .streamName("quotestream")
            .build();

        Stream.Builder.create(this, "positions")
            .streamName("positions")
            .build();

        Stream.Builder.create(this, "marketval")
            .streamName("marketval")
            .build();
    }
}
