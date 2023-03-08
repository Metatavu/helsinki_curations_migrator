### Helsinki Curations Migrator

This application fetches Curations from Elastic App Search and stores information about them into AWS DynamoDB.

For development purposes install Rust as instructed in [The Book](https://doc.rust-lang.org/book/ch01-01-installation.html).

Built with `cargo build --release`

Usage:
`./helsinki-curations-migrator --url {ELASTIC_APP_SEARCH_BASE_URL} --api-key {ELASTIC_APP_SEARCH_API_KEY_WITH_CURATIONS_RIGHTS} --engine {ELASTIC_APP_SEARCH_ENGINE_NAME} --region {AWS_REGION}`