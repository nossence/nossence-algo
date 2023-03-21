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

type ScoredPost struct {
	Id        string    `json:"event_id"`
	Kind      int       `json:"kind"`
	Pubkey    string    `json:"pubkey"`
	CreatedAt time.Time `json:"created_at"`
	Score     float64   `json:"score"`
}

func NewEngine(driver neo4j.DriverWithContext) *Engine {
	return &Engine{
		driver: driver,
	}
}

func (e *Engine) GetFeed(userPub string, start time.Time, end time.Time, limit int) []ScoredPost {
	ctx := context.Background()

	session := e.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	posts, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		ctx := context.Background()

		result, err := tx.Run(ctx, `
match (p:Post) where p.created_at > $Start and p.created_at < $End
match (u:User)-[:CREATE]->(r:Post)-[l:REPLY|LIKE|ZAP]->(p)
optional match (:User {pubkey: $Pubkey})-[s:SIMILAR]->(u:User)
with p, sum((case when s is not null then s.score * 420 else 1.0 end) * (case when l:REPLY then 3 when l:LIEK then 2 when l:ZAP then 6 end)) as score
order by score desc limit $Limit return p.id, p.kind, p.author, p.created_at, score;
`,
			map[string]any{
				"Start":  start.Unix(),
				"End":    end.Unix(),
				"Pubkey": userPub,
				"Limit":  limit,
			})

		if err != nil {
			return nil, err
		}

		posts := make([]ScoredPost, 0)
		for result.Next(ctx) {
			record := result.Record()
			post := ScoredPost{
				Id:        record.Values[0].(string),
				Kind:      int(record.Values[1].(int64)),
				Pubkey:    record.Values[2].(string),
				CreatedAt: time.Unix(record.Values[3].(int64), 0),
				Score:     record.Values[4].(float64),
			}
			posts = append(posts, post)
		}
		return posts, nil
	})

	if err != nil {
		log.Error("Failed to get feed", "err", err)
		return nil
	} else {
		return posts.([]ScoredPost)
	}
}
