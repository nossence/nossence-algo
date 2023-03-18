package algorithm

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Engine struct {
	driver neo4j.DriverWithContext
}

type FeedEntry struct {
	Id        string    `json:"event_id"`
	Kind      int       `json:"kind"`
	Pubkey    string    `json:"pubkey"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
	Score     int       `json:"score"`
}

func NewEngine(driver neo4j.DriverWithContext) *Engine {
	return &Engine{
		driver: driver,
	}
}

func (e *Engine) GetFeed(userPub string, start time.Time, end time.Time, limit int) []FeedEntry {
	ctx := context.Background()

	session := e.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	posts, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		ctx := context.Background()

		result, err := tx.Run(ctx, "match (p:Post) where p.created_at > $Start and p.created_at < $End optional match (r1:Post)-[:REPLY]->(p) optional match (r2:Post)-[:LIKE]->(p) optional match (r3:Post)-[:ZAP]->(p) with p, count(distinct r1.author)*15+count(distinct r2.author)*10+count(distinct r3.author)*50 as score order by score desc limit $Limit return p.id, p.kind, p.author, p.content, p.created_at, score;",
			map[string]any{
				"Start": start.Unix(),
				"End":   end.Unix(),
				"Limit": limit,
			})

		if err != nil {
			return nil, err
		}

		posts := make([]FeedEntry, 0)
		for result.Next(ctx) {
			record := result.Record()
			post := FeedEntry{
				Id:        record.Values[0].(string),
				Kind:      int(record.Values[1].(int64)),
				Pubkey:    record.Values[2].(string),
				Content:   record.Values[3].(string),
				CreatedAt: time.Unix(record.Values[4].(int64), 0),
				Score:     int(record.Values[5].(int64)),
			}
			posts = append(posts, post)
		}
		return posts, nil
	})

	if err != nil {
		log.Error("Failed to get feed", "err", err)
		return nil
	} else {
		return posts.([]FeedEntry)
	}
}
